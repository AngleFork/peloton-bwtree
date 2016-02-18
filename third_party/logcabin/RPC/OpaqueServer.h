/* Copyright (c) 2011-2014 Stanford University
 * Copyright (c) 2015 Diego Ongaro
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <deque>
#include <memory>
#include <string>
#include <unordered_set>

#include "Core/CompatHash.h"
#include "RPC/MessageSocket.h"

#ifndef LOGCABIN_RPC_OPAQUESERVER_H
#define LOGCABIN_RPC_OPAQUESERVER_H

namespace LogCabin {

// forward declaration
namespace Core {
class Buffer;
};

// forward declaration
namespace Event {
class Loop;
};

namespace RPC {

// forward declarations
class Address;
class OpaqueServerRPC;

/**
 * An OpaqueServer listens for incoming RPCs over TCP connections.
 * OpaqueServers can be created from any thread, but they will always run on
 * the thread running the Event::Loop.
 */
class OpaqueServer {
  public:

    /**
     * An interface for handling events generated by an OpaqueServer.
     * The Handler's lifetime must outlive that of the OpaqueServer.
     */
    class Handler {
      public:
        /**
         * Destructor.
         */
        virtual ~Handler() {}

        /**
         * This method is overridden by a subclass and invoked when a new RPC
         * arrives. This will be called from the Event::Loop thread, so it must
         * return quickly. It should call OpaqueServerRPC::sendReply() if and
         * when it wants to respond to the RPC request.
         */
        virtual void handleRPC(OpaqueServerRPC serverRPC) = 0;
    };

    /**
     * Constructor. This object won't actually do anything until bind() is
     * called.
     * \param handler
     *      Handles inbound RPCs.
     * \param eventLoop
     *      Event::Loop that will be used to find out when the underlying
     *      socket may be read from or written to without blocking.
     * \param maxMessageLength
     *      The maximum number of bytes to allow per request/response. This
     *      exists to limit the amount of buffer space a single RPC can use.
     *      Attempting to send longer responses will PANIC; attempting to
     *      receive longer requests will disconnect the underlying socket.
     */
    OpaqueServer(Handler& handler,
                 Event::Loop& eventLoop,
                 uint32_t maxMessageLength);

    /**
     * Destructor. OpaqueServerRPC objects originating from this OpaqueServer
     * may be kept around after this destructor returns; however, they won't
     * actually send replies anymore.
     */
    ~OpaqueServer();

    /**
     * Listen on an address for new client connections. You can call this
     * multiple times to listen on multiple addresses. (But if you call this
     * twice with the same address, the second time will always throw an
     * error.)
     * This method is thread-safe.
     * \param listenAddress
     *      The TCP address on listen for new client connections.
     * \return
     *      An error message if this was not able to listen on the given
     *      address; the empty string otherwise.
     */
    std::string bind(const Address& listenAddress);

  private:

    // forward declaration
    struct SocketWithHandler;

    /**
     * Receives events from a MessageSocket.
     */
    class MessageSocketHandler : public MessageSocket::Handler {
      public:
        explicit MessageSocketHandler(OpaqueServer* server);
        void handleReceivedMessage(MessageId messageId, Core::Buffer message);
        void handleDisconnect();

        /**
         * The OpaqueServer which keeps a strong reference to this object, or
         * NULL if the server has been/is being destroyed. Used to invoke the
         * server's rpcHandler when receiving an RPC request, or to drop the
         * server's reference to this socket when disconnecting.
         *
         * May only be accessed with an Event::Loop::Lock or from the event
         * loop, since the OpaqueServer may set this to NULL under the same
         * rules.
         */
        OpaqueServer* server;

        /**
         * A weak reference to this object, used to give OpaqueServerRPCs a way
         * to send their replies back on their originating socket.
         * This may be empty when the SocketWithHandler is shutting down.
         */
        std::weak_ptr<SocketWithHandler> self;

        // MessageSocketHandler is not copyable.
        MessageSocketHandler(const MessageSocketHandler&) = delete;
        MessageSocketHandler& operator=(const MessageSocketHandler&) = delete;
    };

    /**
     * Couples a MessageSocketHandler with a MessageSocket (monitor) and
     * destroys them in the right order (monitor first).
     *
     * This class is reference-counted with std::shared_ptr. Usually, one
     * strong reference exists in OpaqueServer::sockets, which keeps this
     * object alive. Weak references exist OpaqueServerRPC objects and in
     * MessageSocketHandler::self (to copy into OpaqueServerRPC objects).
     */
    struct SocketWithHandler {
      public:
        /**
         * Return a newly constructed SocketWithHandler, with the handler's
         * self field pointing to itself.
         * \param server
         *      Server that owns this object. Held by MessageSocketHandler.
         * \param fd
         *      TCP connection with client for MessageSocket.
         */
        static std::shared_ptr<SocketWithHandler>
        make(OpaqueServer* server, int fd);

        ~SocketWithHandler();
        MessageSocketHandler handler;
        MessageSocket monitor;

      private:
        SocketWithHandler(OpaqueServer* server, int fd);
    };

    /**
     * A socket that listens on a particular address.
     */
    class BoundListener : public Event::File {
      public:
        /**
         * Constructor.
         * \param server
         *      OpaqueServer that owns this object.
         * \param fd
         *      The underlying socket that is listening on a particular
         *      address.
         */
        BoundListener(OpaqueServer& server, int fd);
        void handleFileEvent(uint32_t events);
        OpaqueServer& server;
    };

    /**
     * Couples a BoundListener with an Event::File::Monitor and destroys them
     * in the right order (monitor first).
     */
    struct BoundListenerWithMonitor {
        /// Constructor. See BoundListener.
        BoundListenerWithMonitor(OpaqueServer& server, int fd);

        /// Destructor.
        ~BoundListenerWithMonitor();
        /**
         * This creates a new SocketWithHandler instance upon getting a new
         * connection.
         */
        BoundListener handler;
        /**
         * This listens for incoming TCP connections and calls 'handler' with
         * them.
         */
        Event::File::Monitor monitor;
    };

    /**
     * Deals with OpaqueServerRPC objects that this class creates when it
     * receives a request.
     */
    Handler& rpcHandler;

    /**
     * The event loop that is used for non-blocking I/O.
     */
    Event::Loop& eventLoop;

    /**
     * The maximum number of bytes to allow per request/response.
     */
    const uint32_t maxMessageLength;

    /**
     * Every open socket is referenced here so that it can be cleaned up when
     * this OpaqueServer is destroyed. These are reference-counted: the
     * lifetime of each socket may slightly exceed the lifetime of the
     * OpaqueServer if it is being actively used to send out a OpaqueServerRPC
     * response when the OpaqueServer is destroyed.
     *
     * This may only be accessed from the Event::Loop or while holding an
     * Event::Loop::Lock (it's almost entirely accessed from event handlers, so
     * it's convenient to rely on the Event::Loop::Lock for mutual exclusion
     * during OpauqeServer's destructor as well).
     */
    std::unordered_set<std::shared_ptr<SocketWithHandler>> sockets;

    /**
     * Lock to prevent concurrent modification of #boundListeners.
     */
    Core::Mutex boundListenersMutex;

    /**
     * A list of listening sockets that each listen on a particular address.
     * std::deque is used so that the handler objects have a stable memory
     * location (otherwise their monitors would blow up).
     */
    std::deque<BoundListenerWithMonitor> boundListeners;

    /**
     * OpaqueServerRPC keeps a std::weak_ptr back to its originating
     * ServerMessageSocket.
     */
    friend class OpaqueServerRPC;

    // OpaqueServer is non-copyable.
    OpaqueServer(const OpaqueServer&) = delete;
    OpaqueServer& operator=(const OpaqueServer&) = delete;
}; // class OpaqueServer

} // namespace LogCabin::RPC
} // namespace LogCabin

#endif /* LOGCABIN_RPC_OPAQUESERVER_H */
