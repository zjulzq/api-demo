package demo.jeromq;

import org.zeromq.ZMQ;

public class Pair {
	private static final String addr = "tcp://localhost:5555";

	public static void main(String[] args) {
		Pair pair = new Pair();
		pair.createClient();
		pair.createServer();
	}

	public void createClient() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket client = context.socket(ZMQ.PAIR);
				client.bind(addr);
				while (!Thread.interrupted()) {
					client.send("Hello World.");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				client.close();
				context.close();
			}
		}).start();
	}

	public void createServer() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket server = context.socket(ZMQ.PAIR);
				server.connect(addr);
				while (!Thread.interrupted()) {
					System.out.println(server.recvStr());
				}
				server.close();
				context.close();
			}
		}).start();
	}
}
