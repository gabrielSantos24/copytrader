# Rithmic Latency Benchmark POC

POC em Rust para produzir dados tecnicamente defensaveis sobre a localizacao do futuro **Execution Plane** de uma plataforma de copy trading.

A pergunta nao e apenas "qual VPS tem o menor ping?". A pergunta e:

> Que localizacao deve alojar o Execution Plane, considerando latencia aplicacional, jitter, estabilidade da ligacao e complexidade operacional?

O mesmo binario deve ser executado, com a mesma metodologia, em Lisboa, Nova Iorque, Chicago e restantes candidatos. Os dados, e nao uma preferencia previa por Chicago, devem orientar a decisao. O Control Plane e o Execution Plane continuam fora do scope deste POC.

## Conclusao que o POC pode suportar

O benchmark permite comparar o caminho entre cada VPS e o gateway Rithmic Test. Pode sustentar uma decisao como:

```text
Execution Plane -> Chicago ou outra regiao dos EUA
Control Plane   -> Europa
```

ou manter ambos na Europa, caso a diferenca medida nao seja material, repetivel ou operacionalmente vantajosa.

Nao mede a latencia end-to-end de copiar uma ordem. Essa incluiria rececao do fill, regras de routing, criacao da ordem destino, envio, processamento do broker/exchange e confirmacao.

## R|Protocol implementado

O transporte e WebSocket TLS com mensagens **binarias Google Protocol Buffers**. Nao existe JSON nem framing Tradovate.

Fluxo:

```text
CONNECT discovery WebSocket
  -> RequestRithmicSystemInfo (16)
  -> ResponseRithmicSystemInfo (17)
  -> validar "Rithmic Test"
  -> fechar

CONNECT benchmark WebSocket
  -> RequestLogin (10), Order Plant, template 5.55
  -> ResponseLogin (11)
  -> validar rp_code == ["0"]
  -> obter heartbeat_interval
  -> WARM-UP
  -> RTT BENCHMARK
  -> optional stability soak
  -> optional account discovery/fill subscription
  -> SHUTDOWN
```

A segunda WebSocket permanece aberta durante todas as fases medidas. Nao existe reconnect automatico: um disconnect termina a run como `partial`. Misturar samples de ligacoes diferentes esconderia uma falha importante e contaminaria a comparacao.

## Request escolhido para RTT

O RTT usa:

- `RequestHeartbeat`, template 18;
- `ResponseHeartbeat`, template 19;
- `user_msg` unico para correlacao request/response.

Foi escolhido porque e oficial, read-only, nao coloca ordens, nao altera configuracao e tem resposta direta. O guia descreve heartbeat como mecanismo para monitorizar/manter a ligacao; o schema permite que `user_msg` seja devolvido na resposta.

E uma medicao de **application-level R|Protocol request/response RTT**. Inclui runtime local, WebSocket/TLS ja estabelecido, rede, gateway e processamento do heartbeat. Nao e ICMP/network RTT puro e pode nao seguir exatamente o mesmo caminho de processamento de uma ordem.

A cadencia default e controlada: um request outstanding, resposta, pausa de 250 ms, proximo request. Nao existe concorrencia artificial. A aceitabilidade operacional desta frequencia deve ser confirmada com a Rithmic antes de campanhas prolongadas.

O socket usa TCP `NODELAY` para evitar que Nagle introduza batching artificial em mensagens pequenas; esta definicao fica registada no summary e e igual em todas as regioes.

## Fronteira temporal

Para cada tentativa:

```text
serializar protobuf
T0 = Instant::now()
WebSocket send
aguardar ResponseHeartbeat com o user_msg esperado
T1 = Instant capturado assim que a frame binaria chega ao processo Rust
RTT = T1 - T0
```

`std::time::Instant` e a unica fonte usada no calculo do RTT. UTC serve apenas como metadata. Descodificacao posterior, logging, estatistica e escrita em disco ficam fora da duracao.

## Controlo de contaminacao

A metodologia default e:

1. **CONNECT**: TLS/WebSocket e SystemInfo fora das estatisticas.
2. **AUTHENTICATE**: Login fora das estatisticas.
3. **WARM-UP**: 50 heartbeats completos, nao guardados.
4. **BENCHMARK**: 1000 tentativas sequenciais.
5. **STABILITY**: soak opcional, fora da distribuicao RTT.
6. **FILL**: opcional e iniciado apenas depois do RTT.
7. **SHUTDOWN**: fecho intencional.
8. **EXPORT**: CSV/JSONL/summary escritos depois das fases medidas.

Nao ha escrita em disco por sample no hot path. O logging ocorre apenas por blocos de 100 tentativas. Rust nao tem garbage collector. Outliers nao sao removidos.

Um timeout e guardado no raw output com `outcome=timeout` e `latency_ms=null`; a serie continua. Um disconnect/read/write/protocol error e fatal e produz output parcial. `Ctrl+C` fecha a ligacao e exporta os samples ja mantidos em memoria.

## Estatisticas

Calculadas apenas sobre RTTs bem-sucedidos:

- count;
- min e max;
- media;
- desvio padrao populacional;
- mean absolute successive delta, uma medida explicita de variabilidade entre samples consecutivos;
- P50, P95 e P99 por nearest-rank.

```text
rank = ceil(percentile * N)
value = sorted[rank - 1]
```

O summary preserva separadamente tentativas pedidas, completadas, nao completadas, sucessos, timeouts e contadores. Nenhum outlier e eliminado silenciosamente.

## Connection stability

Sao exportados:

- connections abertas, separando discovery e benchmark;
- connect failures;
- intentional closes;
- disconnects e unexpected closes;
- reconnects;
- timeouts;
- read/write/protocol errors;
- authentication failures;
- mensagens inesperadas;
- heartbeats enviados/recebidos;
- WebSocket pings recebidos.

`--stability-secs` mantem a mesma ligacao aberta depois do RTT e envia heartbeat apenas segundo o intervalo devolvido pelo Login. Para avaliar um processo de longa duracao, usar a mesma duracao em todas as regioes, por exemplo `--stability-secs 3600`.

## Fill Event Age: experimental

A recolha de fills esta desligada por defeito: `--fill-samples 0`.

Quando ativada, o cliente so depois do RTT executa `RequestLoginInfo`, lista/valida a conta e envia `RequestSubscribeForOrderUpdates`. Aceita apenas `ExchangeOrderNotification` (352) com `notify_type=FILL`, conta exata, fill novo e nao-snapshot.

O schema contem `source_ssboe`, `source_usecs`, `source_nsecs` e timestamps adicionais. Contudo, o guia fornecido nao define suficientemente a semantica de origem desses campos para este evento. Alem disso:

```text
UTC local - timestamp remoto
```

depende do offset e drift entre relogios.

Por isso o output chama-se **Fill Event Age**, fica marcado como experimental e nao deve ser usado como one-way latency nem como conclusao principal.

**BLOCKED:** confirmar por documentacao/suporte Rithmic a semantica exata dos timestamps de `ExchangeOrderNotification` e estabelecer um procedimento mensuravel de clock synchronization/offset antes de interpretar esta metrica como latencia.

## Compilar

O SDK oficial e confidencial e nao e versionado. Neste workspace encontra-se em:

```text
docs/rithmic/sdk/0.90.0.0/proto
```

Noutro computador, extrair o SDK autorizado e definir:

```powershell
$env:RITHMIC_PROTO_DIR="C:\caminho\RProtocolAPI\proto"
```

Depois:

```powershell
& "$env:USERPROFILE\.cargo\bin\cargo.exe" fmt --check
& "$env:USERPROFILE\.cargo\bin\cargo.exe" check
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test
& "$env:USERPROFILE\.cargo\bin\cargo.exe" build --release
```

O binario release e autonomo; o SDK nao e necessario no VPS.

## Executar RTT-only

As credenciais sao lidas apenas de environment variables:

```powershell
$env:RITHMIC_USERNAME="LOGIN_RITHMIC_TEST"
$env:RITHMIC_PASSWORD="PASSWORD_RITHMIC_TEST"

.\target\release\rithmic-latency-poc.exe `
  --location lisbon `
  --rtt-samples 1000
```

Linux:

```bash
export RITHMIC_USERNAME='LOGIN_RITHMIC_TEST'
export RITHMIC_PASSWORD='PASSWORD_RITHMIC_TEST'

./rithmic-latency-poc --location chicago --rtt-samples 1000
```

As credenciais do portal de download do SDK podem ser diferentes do login Rithmic Test. Nunca colocar passwords em flags, README, outputs ou Git.

Exemplo com soak de uma hora:

```bash
./rithmic-latency-poc \
  --location new-york \
  --warmup-samples 50 \
  --rtt-samples 1000 \
  --rtt-interval-ms 250 \
  --stability-secs 3600
```

Fill Event Age opcional:

```bash
export RITHMIC_SOURCE_ACCOUNT_ID='TEST_ACCOUNT'
./rithmic-latency-poc --location chicago --fill-samples 20 --fill-wait-secs 1800
```

O POC nunca coloca ordens; os fills devem ser gerados manualmente noutra aplicacao de teste autorizada.

## Outputs

```text
results/
  <location>_<timestamp>.csv
  <location>_<timestamp>.jsonl
  <location>_<timestamp>_summary.json
```

CSV/JSONL preservam cada sucesso e timeout. O summary inclui:

- methodology/program version;
- location, system, endpoint e template version;
- host, OS, arquitetura e PID;
- inicio/fim da run e da fase RTT;
- warm-up, tentativas, intervalo e timeouts configurados;
- request escolhido e politica de uma request outstanding;
- heartbeat interval devolvido pelo servidor;
- estatisticas;
- estado `completed` ou `partial`;
- contadores de estabilidade;
- erro final sem credenciais.

## Procedimento para comparar regioes

1. Compilar uma vez e distribuir exatamente o mesmo binario.
2. Registar e comparar o SHA-256 do binario em todos os VPS.
3. Usar o mesmo provider/instance class, OS, CPU policy e configuracao de rede sempre que possivel.
4. Usar exatamente warm-up, attempts, intervalo, timeout e soak iguais.
5. Alterar apenas `--location` como metadata.
6. Evitar outras cargas no host durante a run.
7. Executar varias runs por regiao e alternar a ordem/horario entre regioes para reduzir bias temporal.
8. Guardar todos os raw files; nao comparar apenas uma P50.
9. Comparar P50/P95/P99, media, stddev, variabilidade, timeouts, disconnects e run-to-run variance.
10. Considerar material apenas uma vantagem repetivel e maior que a variacao observada.

A localizacao nao e literalmente a unica variavel fisica: provider, hardware, routing, hora e carga Rithmic tambem influenciam. A metodologia procura controla-los e expo-los, nao fingir que nao existem.

## Configuracao

| Variavel/flag | Default | Funcao |
|---|---:|---|
| `--location` / `RITHMIC_LOCATION` | obrigatoria | Metadata da regiao |
| `RITHMIC_USERNAME` | obrigatoria | Login Rithmic Test |
| `RITHMIC_PASSWORD` | obrigatoria | Password Rithmic Test |
| `--system-name` | `Rithmic Test` | Sistema validado por SystemInfo |
| `--warmup-samples` | `50` | RTTs descartados antes da medicao |
| `--rtt-samples` | `1000` | Numero de tentativas guardadas |
| `--rtt-interval-ms` | `250` | Pausa depois de cada resposta/timeout |
| `--request-timeout-secs` | `5` | Timeout de request |
| `--connect-timeout-secs` | `30` | Timeout de connect/TLS |
| `--stability-secs` | `0` | Soak opcional |
| `--fill-samples` | `0` | Fill Event Age opcional |
| `--source-account-id` | unset | Obrigatorio apenas para fills |
| `--fill-wait-secs` | `0` | 0 espera indefinidamente |
| `--output-dir` | `results` | Pasta de output |

Valores abaixo de 1000 RTTs sao permitidos para smoke tests, mas nao devem ser usados na comparacao oficial.

## Estrutura

- `src/main.rs`: orchestration das fases e export parcial.
- `src/cli.rs` e `src/config.rs`: inputs, secrets e validacao.
- `src/rithmic/websocket.rs`: transporte binario, T1, timeouts e ping/pong.
- `src/rithmic/protocol.rs`: template IDs e `rp_code`.
- `src/rithmic/messages.rs`: parsing validado de SystemInfo/Login/Heartbeat.
- `src/rithmic/session.rs`: discovery, login e subscricao opcional.
- `src/rithmic/events.rs`: identificacao/deduplicacao de fills.
- `src/benchmark/rtt.rs`: warm-up e RTT sequencial.
- `src/benchmark/stability.rs`: soak opcional.
- `src/benchmark/fill_event.rs`: Fill Event Age experimental.
- `src/benchmark/statistics.rs`: distribuicao e variabilidade.
- `src/models.rs`: schemas de output.
- `src/output.rs`: export pos-benchmark.
- `build.rs`: gera Rust a partir do SDK autorizado.

## Limites e pontos BLOCKED

- **BLOCKED runtime:** autenticar e recolher samples reais exige credenciais Rithmic Test e conta autorizada; testes unitarios nao substituem esta validacao.
- **BLOCKED cadence approval:** o SDK nao publica um rate limit para usar heartbeat como benchmark; confirmar com a Rithmic que a cadencia escolhida e aceitavel.
- **BLOCKED fill one-way latency:** sem semantica oficial suficiente e clock-offset medido, Fill Event Age nao e one-way latency.
- O endpoint Test pode ter topologia, carga e routing diferentes de live/prop-firm.
- Heartbeat RTT representa o caminho aplicacional do heartbeat, nao order placement/fill end-to-end.
- TLS handshake, SystemInfo, Login e reconnect ficam deliberadamente fora da distribuicao RTT.
- O benchmark nao mede processamento futuro do copier nem a latencia da conta destino.
- Um soak curto nao prova estabilidade por dias; executar runs prolongadas e repetidas.
- A decisao final deve combinar estes dados com custos, operacao, resiliencia e requisitos regulatorios.
