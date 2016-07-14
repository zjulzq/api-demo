package demo.jeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class ReqRep {
	private static final Logger log = LoggerFactory.getLogger(ReqRep.class);
	private String bindAddr = "tcp://*:5555";
	private String connectAddr = "tcp://localhost:5555";

	public static void main(String[] args) {
		ReqRep reqRep = new ReqRep();
		reqRep.startup(2);
	}

	public void startup(int reqs) {
		createRep();
		createReq(reqs);
	}

	private void createRep() {
		String threadName = "rep";
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket rep = context.socket(ZMQ.REP);
				rep.bind(bindAddr);
				String world = "World";
				while (!Thread.currentThread().isInterrupted()) {
					String req = rep.recvStr();
					if (req != null && req.equals("Hello")) {
						rep.send(world.getBytes());
					}
				}
				rep.close();
				context.close();
			}
		}, threadName).start();
	}

	private void createReq(int reqs) {
		for (int i = 0; i < reqs; i++) {
			final String threadName = "req" + i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					ZMQ.Context context = ZMQ.context(1);
					ZMQ.Socket req = context.socket(ZMQ.REQ);
					req.connect(connectAddr);
					while (!Thread.currentThread().isInterrupted()) {
						try {
							Thread.sleep((long) (Math.floor(Math.random() * 100) + 100));
						} catch (InterruptedException e) {
							log.warn("", e);
						}
						req.send("Hello".getBytes(), 0);
						String rep = req.recvStr();
						log.info("{} receive response: {}.", threadName, rep);
					}
					req.close();
					context.close();
				}
			}, threadName).start();
		}
	}
}
