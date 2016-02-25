package demo.jeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class XsubXpub {
	private static final Logger log = LoggerFactory.getLogger(XsubXpub.class);
	private static final long HWM = 1000;
	private String pubAddr = "tcp://localhost:5555";
	private String subAddr = "tcp://localhost:6666";
	private String channel = "[test]";

	public static void main(String[] args) {
		XsubXpub xsubXpub = new XsubXpub();
		xsubXpub.startup(1, 1);
	}

	public void startup(int pubs, int subs) {
		for (int i = 0; i < pubs; i++) {
			createPub(i);
		}
		for (int i = 0; i < subs; i++) {
			createSub(i);
		}

		ZMQ.Context context1 = ZMQ.context(1);
		final ZMQ.Socket xsub = context1.socket(ZMQ.XSUB);
		xsub.bind(pubAddr);
		xsub.setHWM(HWM);

		ZMQ.Context context2 = ZMQ.context(1);
		final ZMQ.Socket xpub = context2.socket(ZMQ.XPUB);
		xpub.bind(subAddr);
		xpub.setHWM(HWM);
		xpub.setXpubVerbose(true);
		xpub.setSendTimeOut(0);

		String xsubThreadName = "xsub";
		Thread xsubThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					String msg = xsub.recvStr();
					if (msg != null) {
						xpub.send(msg.getBytes());
					}
				}
			}
		}, xsubThreadName);
		xsubThread.start();

		String xpubThreadName = "xpub";
		Thread xpubThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					String msg = xpub.recvStr();
					if (msg != null) {
						if (msg.getBytes()[0] == 0) {
							log.info("unsubscribe {}", msg.substring(1));
						} else {
							log.info("subscribe {}", msg.substring(1));
						}
						xsub.send(msg.getBytes());
					}
				}
			}
		}, xpubThreadName);
		xpubThread.start();

		while (!xsubThread.isInterrupted() || !xpubThread.isInterrupted()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.warn("", e);
			}
		}

		xpub.close();
		context2.close();

		xsub.close();
		context1.close();
	}

	private void createPub(int id) {
		final String threadName = "pub" + id;
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket pub = context.socket(ZMQ.PUB);
				pub.connect(pubAddr);
				pub.setHWM(HWM);
				pub.setSendTimeOut(0);
				String msg = String.format("%s %s", channel, threadName);
				while (!Thread.currentThread().isInterrupted()) {
					try {
						Thread.sleep(250);
					} catch (InterruptedException e) {
						log.warn("", e);
					}
					pub.send(msg.getBytes());
				}
				pub.close();
				context.close();
			}
		}, threadName).start();
	}

	private void createSub(int id) {
		final String threadName = "sub" + id;
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket sub = context.socket(ZMQ.SUB);
				sub.connect(subAddr);
				sub.setHWM(HWM);
				sub.subscribe(channel.getBytes());
				while (!Thread.currentThread().isInterrupted()) {
					String msg = sub.recvStr();
					log.info("{} receive msg: {}", threadName, msg);
				}
				sub.close();
				context.close();
			}
		}, threadName).start();
	}

}
