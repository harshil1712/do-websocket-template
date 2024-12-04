import { DurableObject } from 'cloudflare:workers';

// Worker
export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		if (request.url.endsWith('/websocket')) {
			// Expect to receive a WebSocket Upgrade request.
			// If there is one, accept the request and return a WebSocket Response.
			const upgradeHeader = request.headers.get('Upgrade');
			if (!upgradeHeader || upgradeHeader !== 'websocket') {
				return new Response('Worker expected Upgrade: websocket', {
					status: 426,
				});
			}

			if (request.method !== 'GET') {
				return new Response('Worker expected GET method', {
					status: 400,
				});
			}

			// Since we are hard coding the Durable Object ID by providing the constant name 'foo',
			// all requests to this Worker will be sent to the same Durable Object instance.
			let id = env.WEBSOCKET_SERVER.idFromName('foo');
			let stub = env.WEBSOCKET_SERVER.get(id);

			return stub.fetch(request);
		}

		return new Response(
			`Supported endpoints:
/websocket: Expects a WebSocket upgrade request`,
			{
				status: 200,
				headers: {
					'Content-Type': 'text/plain',
				},
			}
		);
	},
};

// Durable Object
export class WebSocketServer extends DurableObject {
	// Keeps track of all WebSocket connections
	sessions: Map<WebSocket, { [key: string]: string }>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.sessions = new Map();
	}

	async fetch(request: Request): Promise<Response> {
		// Creates two ends of a WebSocket connection.
		const webSocketPair = new WebSocketPair();
		const [client, server] = Object.values(webSocketPair);

		// Calling `accept()` tells the runtime that this WebSocket is to begin terminating
		// request within the Durable Object. It has the effect of "accepting" the connection,
		// and allowing the WebSocket to send and receive messages.
		server.accept();

		// Generate a random UUID for the session.
		const id = crypto.randomUUID();
		// Add the WebSocket connection to the map of active sessions.
		this.sessions.set(server, { id });

		server.addEventListener('message', (event) => {
			this.handleWebSocketMessage(server, event.data);
		});

		// If the client closes the connection, the runtime will close the connection too.
		server.addEventListener('close', () => {
			this.handleConnectionClose(server);
		});

		return new Response(null, {
			status: 101,
			webSocket: client,
		});
	}

	async handleWebSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
		const connection = this.sessions.get(ws)!;

		// Reply back with the same message to the connection
		ws.send(`[Durable Object] message: ${message}, from: ${connection.id}`);

		// Broadcast the message to all the connections,
		// except the one that sent the message.
		this.sessions.forEach((k, session) => {
			if (session !== ws) {
				session.send(`[Durable Object] message: ${message}, from: ${connection.id}`);
			}
		});

		// Broadcast the message to all the connections,
		// including the one that sent the message.
		this.sessions.forEach((k, session) => {
			session.send(`[Durable Object] message: ${message}, from: ${connection.id}`);
		});
	}

	async handleConnectionClose(ws: WebSocket) {
		this.sessions.delete(ws);
		ws.close(1000, 'Durable Object is closing WebSocket');
	}
}
