using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Used to registry custom methods to refresh at runtime.</summary>
	/// <remarks>
	/// Used to registry custom methods to refresh at runtime.
	/// Each identifier maps to one or more RefreshHandlers.
	/// </remarks>
	public class RefreshRegistry
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RefreshRegistry
			)));

		private class RegistryHolder
		{
			public static org.apache.hadoop.ipc.RefreshRegistry registry = new org.apache.hadoop.ipc.RefreshRegistry
				();
			// Used to hold singleton instance
		}

		// Singleton access
		public static org.apache.hadoop.ipc.RefreshRegistry defaultRegistry()
		{
			return org.apache.hadoop.ipc.RefreshRegistry.RegistryHolder.registry;
		}

		private readonly com.google.common.collect.Multimap<string, org.apache.hadoop.ipc.RefreshHandler
			> handlerTable;

		public RefreshRegistry()
		{
			handlerTable = com.google.common.collect.HashMultimap.create();
		}

		/// <summary>Registers an object as a handler for a given identity.</summary>
		/// <remarks>
		/// Registers an object as a handler for a given identity.
		/// Note: will prevent handler from being GC'd, object should unregister itself
		/// when done
		/// </remarks>
		/// <param name="identifier">
		/// a unique identifier for this resource,
		/// such as org.apache.hadoop.blacklist
		/// </param>
		/// <param name="handler">the object to register</param>
		public virtual void register(string identifier, org.apache.hadoop.ipc.RefreshHandler
			 handler)
		{
			lock (this)
			{
				if (identifier == null)
				{
					throw new System.ArgumentNullException("Identifier cannot be null");
				}
				handlerTable.put(identifier, handler);
			}
		}

		/// <summary>Remove the registered object for a given identity.</summary>
		/// <param name="identifier">the resource to unregister</param>
		/// <returns>the true if removed</returns>
		public virtual bool unregister(string identifier, org.apache.hadoop.ipc.RefreshHandler
			 handler)
		{
			lock (this)
			{
				return handlerTable.remove(identifier, handler);
			}
		}

		public virtual void unregisterAll(string identifier)
		{
			lock (this)
			{
				handlerTable.removeAll(identifier);
			}
		}

		/// <summary>Lookup the responsible handler and return its result.</summary>
		/// <remarks>
		/// Lookup the responsible handler and return its result.
		/// This should be called by the RPC server when it gets a refresh request.
		/// </remarks>
		/// <param name="identifier">the resource to refresh</param>
		/// <param name="args">the arguments to pass on, not including the program name</param>
		/// <exception cref="System.ArgumentException">on invalid identifier</exception>
		/// <returns>the response from the appropriate handler</returns>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshResponse
			> dispatch(string identifier, string[] args)
		{
			lock (this)
			{
				System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshHandler> handlers
					 = handlerTable.get(identifier);
				if (handlers.Count == 0)
				{
					string msg = "Identifier '" + identifier + "' does not exist in RefreshRegistry. Valid options are: "
						 + com.google.common.@base.Joiner.on(", ").join(handlerTable.keySet());
					throw new System.ArgumentException(msg);
				}
				System.Collections.Generic.List<org.apache.hadoop.ipc.RefreshResponse> responses = 
					new System.Collections.Generic.List<org.apache.hadoop.ipc.RefreshResponse>(handlers
					.Count);
				// Dispatch to each handler and store response
				foreach (org.apache.hadoop.ipc.RefreshHandler handler in handlers)
				{
					org.apache.hadoop.ipc.RefreshResponse response;
					// Run the handler
					try
					{
						response = handler.handleRefresh(identifier, args);
						if (response == null)
						{
							throw new System.ArgumentNullException("Handler returned null.");
						}
						LOG.info(handlerName(handler) + " responds to '" + identifier + "', says: '" + response
							.getMessage() + "', returns " + response.getReturnCode());
					}
					catch (System.Exception e)
					{
						response = new org.apache.hadoop.ipc.RefreshResponse(-1, e.getLocalizedMessage());
					}
					response.setSenderName(handlerName(handler));
					responses.add(response);
				}
				return responses;
			}
		}

		private string handlerName(org.apache.hadoop.ipc.RefreshHandler h)
		{
			return Sharpen.Runtime.getClassForObject(h).getName() + '@' + int.toHexString(h.GetHashCode
				());
		}
	}
}
