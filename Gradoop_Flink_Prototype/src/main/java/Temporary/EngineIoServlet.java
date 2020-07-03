package Temporary;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.annotation.WebServlet;

import io.socket.emitter.Emitter;
import io.socket.engineio.server.EngineIoServer;
import io.socket.engineio.server.EngineIoSocket;

@WebServlet("/engine.io/*")
public class EngineIoServlet extends HttpServlet {

	private static final long serialVersionUID = -3704301697786014011L;
	EngineIoServer mEngineIoServer = new EngineIoServer();

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
        mEngineIoServer.handleRequest(request, response);
    
	mEngineIoServer.on("connection", new Emitter.Listener() {
        @Override
        public void call(Object... args) {
            EngineIoSocket socket = (EngineIoSocket) args[0];
            socket.on("message", new Emitter.Listener() {
                @Override
                public void call(Object... args) {
                    Object message = args[0];
                    // message can be either String or byte[]
                    // Do something with message.
                }
            });
        }
	});
    
    }
}
