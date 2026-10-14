use crate::models::MetricStats;

pub(crate) const PERCENTILE_METHOD: &str =
    "nearest-rank: rank=ceil(p*N), using the value at one-based rank";
pub(crate) const STDDEV_METHOD: &str = "population standard deviation over successful samples";

pub(crate) fn calculate_stats(samples: &[f64]) -> Option<MetricStats> {
    let values = samples
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }

    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    let successive_delta = if values.len() < 2 {
        0.0
    } else {
        values
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).abs())
            .sum::<f64>()
            / (values.len() - 1) as f64
    };

    let mut sorted = values.clone();
    sorted.sort_by(f64::total_cmp);
    Some(MetricStats {
        samples: sorted.len(),
        min_ms: sorted[0],
        mean_ms: mean,
        stddev_ms: variance.sqrt(),
        mean_abs_successive_delta_ms: successive_delta,
        p50_ms: percentile(&sorted, 0.50).expect("non-empty validated percentile input"),
        p95_ms: percentile(&sorted, 0.95).expect("non-empty validated percentile input"),
        p99_ms: percentile(&sorted, 0.99).expect("non-empty validated percentile input"),
        max_ms: sorted[sorted.len() - 1],
        percentile_method: PERCENTILE_METHOD,
        stddev_method: STDDEV_METHOD,
    })
}

pub(crate) fn percentile(sorted_samples: &[f64], probability: f64) -> Option<f64> {
    if sorted_samples.is_empty() || !(0.0..=1.0).contains(&probability) {
        return None;
    }
    let rank = (probability * sorted_samples.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted_samples.len() - 1);
    Some(sorted_samples[index])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank() {
        let samples = (1..=100).map(f64::from).collect::<Vec<_>>();
        assert_eq!(percentile(&samples, 0.50), Some(50.0));
        assert_eq!(percentile(&samples, 0.95), Some(95.0));
        assert_eq!(percentile(&samples, 0.99), Some(99.0));
    }

    #[test]
    fn statistics_preserve_order_for_variability_and_sort_for_percentiles() {
        let stats = calculate_stats(&[4.0, 1.0, 3.0, 2.0]).unwrap();
        assert_eq!(stats.samples, 4);
        assert_eq!(stats.min_ms, 1.0);
        assert_eq!(stats.mean_ms, 2.5);
        assert!((stats.stddev_ms - 1.118_033_988_75).abs() < 1e-9);
        assert!((stats.mean_abs_successive_delta_ms - 2.0).abs() < 1e-9);
        assert_eq!(stats.p50_ms, 2.0);
        assert_eq!(stats.p95_ms, 4.0);
        assert_eq!(stats.p99_ms, 4.0);
        assert_eq!(stats.max_ms, 4.0);
    }

    #[test]
    fn handles_empty_and_single_sample() {
        assert!(calculate_stats(&[]).is_none());
        let stats = calculate_stats(&[12.25]).unwrap();
        assert_eq!(stats.mean_ms, 12.25);
        assert_eq!(stats.stddev_ms, 0.0);
        assert_eq!(stats.mean_abs_successive_delta_ms, 0.0);
        assert_eq!(stats.p99_ms, 12.25);
    }
}
