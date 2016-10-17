Background
==========

This is a simple tool you can use to determine if you can talk to your Rabbit cluster.

It creates a queue, binds to it, and sits and listens for messages.

We use this when testing ACL changes on our load balancers or for general debugging.

Usage
=====

Note: Requires the pika library.

~~~~
  ./rabbit_connectivity_test.py --rabbit_user rabbit_user --rabbit_pass secret_password --rabbit_host host

  Connection Open
  Channel Open
  Exchange Connected or Created
  Queue Connected or Created
  Exchange OK
  Queue OK
  Bind OK
  ^C
  Channel Closed
  Connection Closed
~~~~
  
To Do
=====

 * help
 * requirements file
