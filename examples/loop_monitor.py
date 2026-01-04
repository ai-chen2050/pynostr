#!/usr/bin/env python

import uuid
from datetime import datetime
import tornado.ioloop
from rich.console import Console
from tornado import gen

from pynostr.base_relay import RelayPolicy
from pynostr.key import PrivateKey
from pynostr.event import EventKind
from pynostr.filters import Filters, FiltersList
from pynostr.message_pool import MessagePool
from pynostr.relay import Relay

if __name__ == "__main__":

    console = Console()

    pk = PrivateKey()
    privkey_nsec = pk.bech32() 

    relay_url = input("relay: ")

    # Subscribe to all events of kind EventKind.TEXT_NOTE, not filtered by author
    # Remove limit to continuously receive new events
    filters = FiltersList(
        [Filters(kinds=[EventKind.TEXT_NOTE], limit=1)]
    )

    subscription_id = uuid.uuid1().hex
    io_loop = tornado.ioloop.IOLoop.current()
    message_pool = MessagePool(first_response_only=False)
    policy = RelayPolicy()
    # close_on_eose=False to keep connection open and listen for new events
    r = Relay(relay_url, message_pool, io_loop, policy, timeout=6, close_on_eose=False)

    r.add_subscription(subscription_id, filters)

    console.print(f"[green]Connecting to {relay_url}...[/green]")
    console.print(f"[green]Subscription ID: {subscription_id}[/green]")
    console.print(f"[green]Listening for events of kind 30901...[/green]")
    console.print("[yellow]Press Ctrl+C to exit[/yellow]\n")

    # Track seen event IDs to avoid duplicate prints
    seen_event_ids = set()

    def poll_events():
        """Periodically poll message pool for new events."""
        # Check for new events
        while message_pool.has_events():
            event_msg = message_pool.get_event()
            ev = event_msg.event
            
            if ev.id not in seen_event_ids:
                seen_event_ids.add(ev.id)
                event_time = datetime.fromtimestamp(ev.created_at).strftime('%Y-%m-%d %H:%M:%S')
                console.print(f"[cyan]{'='*60}[/cyan]")
                console.print(f"[bold green]Received event:[/bold green] kind={ev.kind}, id={ev.id[:16]}...")
                console.print(f"[bold]Created at:[/bold] {event_time}")
                console.print(f"[bold]Pubkey:[/bold] {ev.pubkey[:16]}...")
                console.print(f"[bold]Content:[/bold] {ev.content[:200]}..." if len(ev.content) > 200 else f"[bold]Content:[/bold] {ev.content}")
                console.print(f"[bold]Tags:[/bold] {ev.tags}")
        
        # Check for EOSE notices
        while message_pool.has_eose_notices():
            eose = message_pool.get_eose_notice()
            console.print(f"[dim]EOSE received from {eose.url} - now listening for new events...[/dim]")

    @gen.coroutine
    def connect_and_subscribe():
        """Connect to relay."""
        try:
            yield r.connect()
        except Exception as e:
            console.print(f"[red]Connection error: {e}[/red]")

    # Set up periodic callback to poll for events (every 500ms)
    callback = tornado.ioloop.PeriodicCallback(poll_events, 500)
    callback.start()

    # Spawn the connection in background (non-blocking)
    io_loop.spawn_callback(connect_and_subscribe)

    try:
        # Keep the io_loop running to receive WebSocket messages
        io_loop.start()
    except KeyboardInterrupt:
        console.print("\n[red]Shutting down...[/red]")
    finally:
        callback.stop()
        r.close()
        io_loop.stop()
        console.print(f"[green]Total unique events received: {len(seen_event_ids)}[/green]")