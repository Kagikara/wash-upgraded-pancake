use std::time::Instant;

/// Lightweight benchmark helper for rule contract tests.
/// Returns elapsed milliseconds for `runs` executions.
pub fn benchmark_runs<F>(runs: usize, mut run_once: F) -> u128
where
    F: FnMut(),
{
    let start = Instant::now();
    for _ in 0..runs {
        run_once();
    }
    start.elapsed().as_millis()
}

#[macro_export]
macro_rules! define_rule_extension_test_suite {
    ($module:ident, $param_test:ident, $hit_test:ident, $false_positive_test:ident, $perf_test:ident) => {
        mod $module {
            #[test]
            fn parameter_validation() {
                super::$param_test();
            }

            #[test]
            fn hit_case() {
                super::$hit_test();
            }

            #[test]
            fn false_positive_case() {
                super::$false_positive_test();
            }

            #[test]
            #[ignore = "performance baseline template; run explicitly when extending rules"]
            fn performance_baseline() {
                super::$perf_test();
            }
        }
    };
}
