use ConnectionError;
use proto::*;

use std::cmp;

#[derive(Copy, Clone, Debug)]
pub struct FlowControl {
    /// Window size as indicated by the peer. This can go negative.
    window_size: i32,

    /// The amount of the window that is currently available to consume.
    available: WindowSize,
}

impl FlowControl {
    pub fn new() -> FlowControl {
        FlowControl {
            window_size: 0,
            available: 0,
        }
    }

    /// Returns the window size as known by the peer
    pub fn window_size(&self) -> WindowSize {
        if self.window_size < 0 {
            0
        } else {
            self.window_size as WindowSize
        }
    }

    /// Returns the window size available to the consumer
    pub fn available(&self) -> WindowSize {
        self.available
    }

    /// Returns true if there is unavailable window capacity
    pub fn has_unavailable(&self) -> bool {
        if self.window_size < 0 {
            return false;
        }

        self.window_size as WindowSize > self.available
    }

    pub fn claim_capacity(&mut self, capacity: WindowSize) {
        assert!(self.available >= capacity);
        self.available -= capacity;
    }

    pub fn assign_capacity(&mut self, capacity: WindowSize) {
        assert!(self.window_size() >= self.available + capacity);
        self.available += capacity;
    }

    /// Update the window size.
    ///
    /// This is called after receiving a WINDOW_UPDATE frame
    pub fn inc_window(&mut self, sz: WindowSize) -> Result<(), ConnectionError> {
        // TODO: Handle invalid increment
        self.window_size += sz as i32;
        Ok(())
    }

    /// Decrements the window reflecting data has actually been sent. The caller
    /// must ensure that the window has capacity.
    pub fn send_data(&mut self, sz: WindowSize) {
        assert!(sz <= self.window_size as WindowSize);
        self.window_size -= sz as i32;
    }

    /// Decrements the **available** window.
    ///
    /// This does not decrement the actual window as visible to the peer. This
    /// function should be called before sending data into the prioritization
    /// layer.
    pub fn buffer_data<E>(&mut self, sz: WindowSize, err: E) -> Result<(), E>
    {
        if self.available < sz {
            return Err(err);
        }

        self.available -= sz;
        Ok(())
    }
}
