// src/durableobjects/LiveMatchDO.ts

import { LiveMatchState, Env } from '../types';

// Define the Durable Object class
export class LiveMatchDO implements DurableObject {
  state: DurableObjectState;
  env: Env; // Access to bindings like DB
  clients: Set<WebSocket>; // Set to store connected WebSocket clients
  liveMatchState: LiveMatchState | null; // Current state of the match

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.clients = new Set();
    this.liveMatchState = null; // Initialize state as null
    // Optional: Restore state from storage if needed
    // this.state.storage.get<LiveMatchState>('liveMatchState').then(storedState => {
    //     this.liveMatchState = storedState;
    // });
  }

  // Handle incoming requests to the Durable Object
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // Handle WebSocket upgrade request (from Worker's /api/live/ws/:matchId)
    if (pathname === '/websocket' && request.headers.get('Upgrade') === 'websocket') {
      console.log(`DO ${this.state.id}: Received WebSocket upgrade request`);
      // Create a WebSocketPair
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      // Accept the WebSocket connection on the server side
      server.accept();

      // Add the server WebSocket to our set of connected clients
      this.clients.add(server);
      console.log(`DO ${this.state.id}: Client connected. Total clients: ${this.clients.size}`);

      // Set up event listeners for the server WebSocket
      server.addEventListener('message', event => {
        // Handle messages from the client if needed (e.g., ping/pong)
        console.log(`DO ${this.state.id}: Received message from client: ${event.data}`);
        // For a live feed, clients usually don't send messages, or maybe just a 'ping'
        // server.send('pong'); // Example response
      });

      server.addEventListener('close', event => {
        // Remove the client when the connection is closed
        this.clients.delete(server);
        console.log(`DO ${this.state.id}: Client disconnected. Total clients: ${this.clients.size}`);
      });

      server.addEventListener('error', event => {
        // Log errors and remove the client
        console.error(`DO ${this.state.id}: WebSocket error:`, event.error);
        this.clients.delete(server);
        console.log(`DO ${this.state.id}: Client error, disconnected. Total clients: ${this.clients.size}`);
      });

      // Send the current state to the newly connected client immediately
      if (this.liveMatchState) {
          try {
              server.send(JSON.stringify(this.liveMatchState));
              console.log(`DO ${this.state.id}: Sent initial state to new client.`);
          } catch (error) {
              console.error(`DO ${this.state.id}: Failed to send initial state to new client:`, error);
              // If sending fails, the connection might be bad, close it.
              server.close(1011, 'Failed to send initial state');
              this.clients.delete(server);
          }
      } else {
          // If state is not loaded yet, maybe fetch it from DB?
          // Or wait for the first update request from the Worker (e.g., match start).
          console.log(`DO ${this.state.id}: No live match state available yet for new client.`);
      }


      // Return the client WebSocket to the Worker, which will return it to the browser
      return new Response(null, { status: 101, webSocket: client });
    }

    // Handle internal requests from the Worker to update state (e.g., after recording a turn)
    if (pathname === '/update-state' && request.method === 'POST') {
        console.log(`DO ${this.state.id}: Received state update request from Worker`);
        try {
            const newState: LiveMatchState = await request.json();
            this.liveMatchState = newState;
            // Optional: Persist state to storage (consider frequency vs cost)
            // await this.state.storage.put('liveMatchState', this.liveMatchState);
            console.log(`DO ${this.state.id}: Updated live match state.`);
            this.broadcastState(); // Broadcast the new state to all connected clients
            return new Response('State updated and broadcasted', { status: 200 });
        } catch (error: any) {
            console.error(`DO ${this.state.id}: Failed to update state:`, error);
            return new Response('Failed to update state', { status: 500, body: error.message });
        }
    }

     // Handle internal requests from the Worker to get state (for HTTP /api/live/state/:matchId)
     if (pathname === '/get-state' && request.method === 'GET') {
         console.log(`DO ${this.state.id}: Received get state request from Worker`);
         if (this.liveMatchState) {
             return new Response(JSON.stringify(this.liveMatchState), {
                 status: 200,
                 headers: { 'Content-Type': 'application/json' }
             });
         } else {
             // If state is not in memory, try fetching from storage or DB if necessary
             // For simplicity here, we'll just return 404 if not in memory
             console.log(`DO ${this.state.id}: State not found in memory for get request.`);
             return new Response('Live state not available for this match', { status: 404 });
         }
     }


    // Handle other requests (e.g., 404)
    return new Response('Durable Object Endpoint Not Found', { status: 404 });
  }

  // Helper method to broadcast the current state to all connected clients
  broadcastState() {
    if (!this.liveMatchState) {
        console.log(`DO ${this.state.id}: No state to broadcast.`);
        return;
    }
    const stateJson = JSON.stringify(this.liveMatchState);
    console.log(`DO ${this.state.id}: Broadcasting state to ${this.clients.size} clients.`);

    // Iterate over clients and send the state
    const clientsToDelete: WebSocket[] = [];
    for (const client of this.clients) {
      try {
        // Check if the connection is still open before sending
        if (client.readyState === WebSocket.OPEN) {
             client.send(stateJson);
        } else {
             // Connection is not open, mark for deletion
             console.warn(`DO ${this.state.id}: Client readyState is ${client.readyState}, marking for deletion.`);
             clientsToDelete.push(client);
        }
      } catch (error) {
        console.error(`DO ${this.state.id}: Failed to send state to client, marking for deletion:`, error);
        // If sending fails, the client is likely disconnected. Mark it for removal.
        clientsToDelete.push(client);
        try {
            client.close(1011, 'Failed to send update'); // Attempt to close gracefully
        } catch (e) {
            // Ignore errors on close if it's already broken
        }
      }
    }

    // Remove clients that failed to receive the message or were not open
    for (const client of clientsToDelete) {
        this.clients.delete(client);
    }
     console.log(`DO ${this.state.id}: Broadcast complete. Remaining clients: ${this.clients.size}`);
  }

  // Optional: Handle DO shutdown (e.g., save state)
  // async alarm() {
  //     // This method is called when an alarm is set
  // }
}
