using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Used to registry custom methods to refresh at runtime.</summary>
	/// <remarks>
	/// Used to registry custom methods to refresh at runtime.
	/// Each identifier maps to one or more RefreshHandlers.
	/// </remarks>
	public class RefreshRegistry
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.RefreshRegistry
			));

		private class RegistryHolder
		{
			public static RefreshRegistry registry = new RefreshRegistry();
			// Used to hold singleton instance
		}

		// Singleton access
		public static RefreshRegistry DefaultRegistry()
		{
			return RefreshRegistry.RegistryHolder.registry;
		}

		private readonly Multimap<string, RefreshHandler> handlerTable;

		public RefreshRegistry()
		{
			handlerTable = HashMultimap.Create();
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
		public virtual void Register(string identifier, RefreshHandler handler)
		{
			lock (this)
			{
				if (identifier == null)
				{
					throw new ArgumentNullException("Identifier cannot be null");
				}
				handlerTable.Put(identifier, handler);
			}
		}

		/// <summary>Remove the registered object for a given identity.</summary>
		/// <param name="identifier">the resource to unregister</param>
		/// <returns>the true if removed</returns>
		public virtual bool Unregister(string identifier, RefreshHandler handler)
		{
			lock (this)
			{
				return handlerTable.Remove(identifier, handler);
			}
		}

		public virtual void UnregisterAll(string identifier)
		{
			lock (this)
			{
				handlerTable.RemoveAll(identifier);
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
		public virtual ICollection<RefreshResponse> Dispatch(string identifier, string[] 
			args)
		{
			lock (this)
			{
				ICollection<RefreshHandler> handlers = handlerTable.Get(identifier);
				if (handlers.Count == 0)
				{
					string msg = "Identifier '" + identifier + "' does not exist in RefreshRegistry. Valid options are: "
						 + Joiner.On(", ").Join(handlerTable.KeySet());
					throw new ArgumentException(msg);
				}
				AList<RefreshResponse> responses = new AList<RefreshResponse>(handlers.Count);
				// Dispatch to each handler and store response
				foreach (RefreshHandler handler in handlers)
				{
					RefreshResponse response;
					// Run the handler
					try
					{
						response = handler.HandleRefresh(identifier, args);
						if (response == null)
						{
							throw new ArgumentNullException("Handler returned null.");
						}
						Log.Info(HandlerName(handler) + " responds to '" + identifier + "', says: '" + response
							.GetMessage() + "', returns " + response.GetReturnCode());
					}
					catch (Exception e)
					{
						response = new RefreshResponse(-1, e.GetLocalizedMessage());
					}
					response.SetSenderName(HandlerName(handler));
					responses.AddItem(response);
				}
				return responses;
			}
		}

		private string HandlerName(RefreshHandler h)
		{
			return h.GetType().FullName + '@' + Sharpen.Extensions.ToHexString(h.GetHashCode(
				));
		}
	}
}
