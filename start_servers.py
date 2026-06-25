#!/usr/bin/env python3
"""
Startup script for running both FastAPI and MCP servers
"""

import subprocess
import sys
import time
import signal
from pathlib import Path

class ServerManager:
    def __init__(self):
        self.processes = []
        self.running = True

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutting down servers...")
        self.running = False
        for process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception:
                pass  # Process might already be dead
        sys.exit(0)

    def start_fastapi_server(self):
        """Start the FastAPI server"""
        print("Starting FastAPI server on port 8000...")
        process = subprocess.Popen([
            sys.executable, "-m", "uvicorn", "server:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--reload"
        ])
        self.processes.append(process)
        return process

    def start_websocket_server(self):
        """Start the WebSocket server"""
        print("Starting WebSocket server on port 5555...")
        process = subprocess.Popen([
            sys.executable, "socketServer.py"
        ])
        self.processes.append(process)
        return process

    def start_mcp_server(self):
        """Start the MCP server"""
        print("Starting MCP server on port 9000...")
        process = subprocess.Popen([
            sys.executable, "mcp_server.py"
        ])
        self.processes.append(process)
        return process

    def start_telegram_bot(self):
        """Start the Telegram Bot"""
        print("Starting Telegram Bot...")
        process = subprocess.Popen([
            sys.executable, "telegram_bot.py"
        ])
        self.processes.append(process)
        return process

    def run(self):
        """Run all servers"""
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        try:
            # Start FastAPI server
            fastapi_process = self.start_fastapi_server()

            # Wait a moment for FastAPI to start
            time.sleep(2)

            # Check if FastAPI started successfully
            if fastapi_process.poll() is not None:
                print("Failed to start FastAPI server")
                return

            print("FastAPI server started successfully!")

            # Start WebSocket server
            websocket_process = self.start_websocket_server()

            # Start MCP server
            mcp_process = self.start_mcp_server()

            # Start Telegram Bot
            telegram_bot_process = self.start_telegram_bot()

            # Wait a moment for WebSocket server to start
            time.sleep(2)

            # Check if WebSocket server started successfully
            if websocket_process.poll() is not None:
                print("Failed to start WebSocket server")
                return

            print("WebSocket server started successfully!")

            print("\nAvailable endpoints:")
            print("  - Health check: http://localhost:8000/health")
            print("  - API docs: http://localhost:8000/docs")
            print("  - Main page: http://localhost:8000/")
            print("  - WebSocket: ws://localhost:5555")
            print("  - MCP server: http://0.0.0.0:9000/mcp")
            print("  - Telegram Bot: Polling active")

            print("\nServers are running. Press Ctrl+C to stop.")

            # Keep the main process alive
            while self.running:
                time.sleep(1)

                # Check if processes are still running
                if fastapi_process.poll() is not None:
                    print("FastAPI server stopped unexpectedly")
                    break

                if websocket_process.poll() is not None:
                    print("WebSocket server stopped unexpectedly")
                    break

                if mcp_process.poll() is not None:
                    print("MCP server stopped unexpectedly")
                    break

                if telegram_bot_process.poll() is not None:
                    print("Telegram Bot stopped unexpectedly")
                    break

        except KeyboardInterrupt:
            pass
        finally:
            self.signal_handler(None, None)

if __name__ == "__main__":
    manager = ServerManager()
    manager.run()
