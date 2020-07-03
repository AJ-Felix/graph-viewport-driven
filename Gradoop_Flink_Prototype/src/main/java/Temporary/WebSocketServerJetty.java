package Temporary;

import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;


public class WebSocketServerJetty {

	
    public static void main(String[] args) throws Exception {
    	BasicConfigurator.configure();
        Server server = new Server(8080);
        WebSocketHandler wsHandler = new WebSocketHandler() {
            @Override
            public void configure(WebSocketServletFactory factory) {
                factory.register(MyWebSocketHandler.class);
            }
        };
        server.setHandler(wsHandler);
        server.start();
        server.join();
    }
}
