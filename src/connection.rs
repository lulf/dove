use crate::conn::ChannelId;
use crate::error::Result;
use crate::framing::{
    AmqpFrame, Attach, Begin, Close, Detach, Disposition, End, Flow, Frame, Open, Performative,
    Transfer,
};
use crate::transport::TransportInfo;
use async_channel::Sender;
use mio::Waker;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    transport: Arc<TransportInfo>,
    waker: Arc<Waker>,
    sender: Sender<Frame>,
}

impl ConnectionHandle {
    pub fn open(&self, open: Open) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Open(open)),
            payload: None,
        })
    }

    pub fn begin(&self, channel: ChannelId, begin: Begin) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Begin(begin)),
            payload: None,
        })
    }

    pub fn attach(&self, channel: ChannelId, attach: Attach) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Attach(attach)),
            payload: None,
        })
    }

    pub fn flow(&self, channel: ChannelId, flow: Flow) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Flow(flow)),
            payload: None,
        })
    }

    pub fn transfer(
        &self,
        channel: ChannelId,
        transfer: Transfer,
        payload: Option<Vec<u8>>,
    ) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Transfer(transfer)),
            payload,
        })
    }

    pub fn disposition(&self, channel: ChannelId, disposition: Disposition) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Disposition(disposition)),
            payload: None,
        })
    }

    pub fn keepalive(&self, remote_idle_timeout: Duration, now: Instant) -> Result<Instant> {
        if remote_idle_timeout.as_millis() > 0 {
            trace!(
                "Remote idle timeout millis: {:?}. Last sent: {:?}",
                remote_idle_timeout.as_millis(),
                now - self.transport.last_sent()
            );

            if now - self.transport.last_sent() >= remote_idle_timeout {
                self.send_amqp_frame(AmqpFrame {
                    channel: 0,
                    performative: None,
                    payload: None,
                })?;
            }
        }
        Ok(self.transport.last_received())
    }

    pub fn detach(&self, channel: ChannelId, detach: Detach) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Detach(detach)),
            payload: None,
        })
    }

    pub fn end(&self, channel: ChannelId, end: End) -> Result<()> {
        self.send_amqp_frame(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::End(end)),
            payload: None,
        })
    }

    pub fn close(&self, close: Close) -> Result<()> {
        let send_result = self.send_amqp_frame(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Close(close)),
            payload: None,
        });
        self.sender.close();
        send_result
    }

    #[inline]
    fn send_amqp_frame(&self, frame: AmqpFrame) -> Result<()> {
        self.sender.try_send(Frame::AMQP(frame))?;
        self.waker.wake()?;
        Ok(())
    }
}

impl From<(Sender<Frame>, (Arc<TransportInfo>, Arc<Waker>))> for ConnectionHandle {
    fn from((sender, (info, waker)): (Sender<Frame>, (Arc<TransportInfo>, Arc<Waker>))) -> Self {
        Self {
            sender,
            transport: info,
            waker,
        }
    }
}
