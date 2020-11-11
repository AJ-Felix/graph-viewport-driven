package Temporary;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.resource;
import static io.undertow.Handlers.websocket;

import java.util.ArrayList;

import io.undertow.Undertow;
import io.undertow.Undertow.ListenerBuilder;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.client.WebSocketClient;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

public class Server2 {
	public ArrayList<WebSocketChannel> channels = new ArrayList<>();
	private int webSocketListenPort2 = 8898;
    private String webSocketHost = "localhost";
	private static Server2 server = null;
    private String webSocketListenPath = "/graphData";
    
    public static Server2 getInstance() {
    	if (server == null) server = new Server2();
		return server;
    }
    
    private Server2() {
    	Undertow server = Undertow.builder()
    			.addHttpListener(webSocketListenPort2, webSocketHost)
                .setHandler(path()
                	.addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
	                    channels.add(channel);
//	                    channel.getReceiveSetter().set(getListener());
//	                    channel.resumeReceives();
	                }))
//                	.addPrefixPath("/", resource(new ClassPathResourceManager(Main.class.getClassLoader(),
//                        Main.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/)
            	).build();
    	server.start();
    }
    
    public void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
}
