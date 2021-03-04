package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class ClientCallHandler extends Thread{
	private Server server;
	private WrapperHandler wrapperHandler;
	private String threadName = "ClientCallHandler";
	private Thread t;
	
	public ClientCallHandler(WrapperHandler wrapperHandler) {
		this.server = Server.getInstance();
		this.wrapperHandler = wrapperHandler;
	}

	@Override
	public void start() {
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}
	
	@Override
	public void run() {
		System.out.println("thread running");
		this.checkForMessages();
	}
	
	private void checkForMessages() {
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		System.out.println("ClientCallHandler in checkFOrMessages");
		Queue<WrapperGVD> queue;
		synchronized (server) {	
			 queue = server.getMessageQueue();
		}
			if (!queue.isEmpty()) {
				Iterator<WrapperGVD> iter = queue.iterator();
				while (iter.hasNext()) {
					System.out.println("Giving to wrapperHandler");
					this.wrapperHandler.addWrapperInitial(iter.next());
					iter.remove();
				}
			} else {
				this.checkForMessages();
			}
//		}
	}
}
