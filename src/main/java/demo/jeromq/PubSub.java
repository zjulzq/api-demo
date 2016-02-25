package demo.jeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class PubSub {
	private static final Logger log = LoggerFactory.getLogger(PubSub.class);
	private String bindAddr = "tcp://*:5555";
	private String connectAddr = "tcp://localhost:5555";
	private String channel = "[test]";

	public static void main(String[] args) {
		PubSub pubSub = new PubSub();
		pubSub.startup(1, 1);
		// pubSub.startup(1, 3);
		// pubSub.startup(3, 1);
	}

	public void startup(int pubs, int subs) {
		boolean pubBind = pubs == 1;
		boolean subBind = !pubBind;
		for (int i = 0; i < pubs; i++) {
			createPub(pubBind, i);
		}
		for (int i = 0; i < subs; i++) {
			createSub(subBind, i);
		}
	}

	private void createPub(final boolean bind, int id) {
		final String threadName = "pub" + id;
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket pub = context.socket(ZMQ.PUB);
				pub.setSendTimeOut(0);
				if (bind) {
					pub.bind(bindAddr);
				} else {
					pub.connect(connectAddr);
				}

				while (!Thread.currentThread().isInterrupted()) {
					try {
						Thread.sleep(250);
					} catch (InterruptedException e) {
						log.warn("", e);
					}
					String msg = String.format("%s Hello, Sub. This is %s.", channel, threadName);
					pub.send(msg.getBytes());
				}
				pub.close();
				context.close();
			}
		}, threadName).start();
	}

	private void createSub(final boolean bind, int id) {
		final String threadName = "sub" + id;
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket sub = context.socket(ZMQ.SUB);
				if (bind) {
					sub.bind(bindAddr);
				} else {
					sub.connect(connectAddr);
				}
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
