use std::sync::Arc;
use std::time::Duration;

use pyo3::{prelude::*, PyResult};

use bamboo_h3_core::clickhouse::window::SlidingWindowOptions;

use crate::clickhouse::ResultSet;
use crate::error::IntoPyResult;
use crate::syncapi::ClickhousePool;

#[pyclass]
pub struct SlidingH3Window {
    inner: Arc<tokio::sync::Mutex<bamboo_h3_core::clickhouse::window::SlidingH3Window>>,
    clickhouse_pool: ClickhousePool,
}

#[pymethods]
impl SlidingH3Window {
    fn fetch_next_window(&mut self, py: Python) -> PyResult<Option<ResultSet>> {
        loop {
            let sw = self.inner.clone();
            let (output, timeouted) = self.clickhouse_pool.runtime.block_on(async move {
                let mut lock = sw.lock().await;
                lock.recv_with_timeout(Duration::from_millis(200)).await
            });

            if timeouted {
                // timeout reached. check if the python program has been interrupted
                // and wait again if that was not the case
                if let Err(e) = py.check_signals() {
                    self.finish_tasks()?;
                    return Err(e);
                }
            } else {
                return match output {
                    Some(rs) => rs.map(|x| Some(ResultSet::from(x))),
                    None => Ok(None),
                }
                .into_pyresult();
            }
        }
    }

    fn close(&mut self) -> PyResult<()> {
        self.finish_tasks()
    }
}

impl SlidingH3Window {
    fn finish_tasks(&mut self) -> PyResult<()> {
        let sw = self.inner.clone();
        self.clickhouse_pool
            .runtime
            .block_on(async move {
                let mut lock = sw.lock().await;
                lock.shutdown().await
            })
            .into_pyresult()
    }
}

impl Drop for SlidingH3Window {
    fn drop(&mut self) {
        let _ = self.finish_tasks();
    }
}

impl SlidingH3Window {
    pub fn create(
        clickhouse_pool: ClickhousePool,
        options: SlidingWindowOptions,
    ) -> PyResult<Self> {
        let pool = clickhouse_pool.pool.clone();
        let inner = clickhouse_pool
            .runtime
            .block_on(async move {
                bamboo_h3_core::clickhouse::window::SlidingH3Window::create(pool, options).await
            })
            .into_pyresult()?;

        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
            clickhouse_pool,
        })
    }
}
