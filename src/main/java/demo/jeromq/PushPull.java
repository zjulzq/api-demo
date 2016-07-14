package demo.jeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class PushPull {
	private static final Logger log = LoggerFactory.getLogger(PushPull.class);
	private String bindAddr = "tcp://*:5555";
	private String connectAddr = "tcp://localhost:5555";

	public static void main(String[] args) {
		PushPull pushPull = new PushPull();
		pushPull.startup(1, 1);
		// pushPull.startup(1, 3);
		// pushPull.startup(3, 1);
		// the pattern of multiple pushes and multiple pulls is not supported.
	}

	public void startup(int pushes, int pulls) {
		boolean pushBind = pushes == 1;
		boolean pullBind = !pushBind;
		for (int i = 0; i < pushes; i++) {
			createPush(pushBind, i);
		}
		for (int j = 0; j < pulls; j++) {
			createPull(pullBind, j);
		}
	}

	private void createPush(final boolean bind, int id) {
		final String threadName = "push" + id;
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket push = context.socket(ZMQ.PUSH);
				push.setSendTimeOut(0);
				if (bind) {
					push.bind(bindAddr);
				} else {
					push.connect(connectAddr);
				}
				int count = 0;
				while (!Thread.currentThread().isInterrupted()) {
					try {
						Thread.sleep(250);
					} catch (InterruptedException e) {
						log.warn("", e);
					}
					String msg = threadName + "-" + count;
					push.send(msg.getBytes(), 0);
					count++;
				}
				push.close();
				context.close();
			}
		}, threadName).start();
	}

	private void createPull(final boolean bind, int id) {
		final String threadName = "pull" + id;
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket pull = context.socket(ZMQ.PULL);
				if (bind) {
					pull.bind(bindAddr);
				} else {
					pull.connect(connectAddr);
				}
				while (!Thread.currentThread().isInterrupted()) {
					String msg = String.valueOf(pull.recvStr(0));
					log.info("{} receive msg: {}.", threadName, msg);
				}
				pull.close();
				context.close();
			}
		}, threadName).start();
	}
}
