package aljoschaRydzyk.viewportDrivenGraphStreaming;

import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;

public class WebSocketCallbackTest implements WebSocketCallback<Void> {
	
	@Override
	public void complete(WebSocketChannel channel, Void context) {
		System.out.println("callback complete");
		
	}

	@Override
	public void onError(WebSocketChannel channel, Void context, Throwable throwable) {
		System.out.println("callback error");		
		
	}

}
