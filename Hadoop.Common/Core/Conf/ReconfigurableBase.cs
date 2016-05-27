using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>Utility base class for implementing the Reconfigurable interface.</summary>
	/// <remarks>
	/// Utility base class for implementing the Reconfigurable interface.
	/// Subclasses should override reconfigurePropertyImpl to change individual
	/// properties and getReconfigurableProperties to get all properties that
	/// can be changed at run time.
	/// </remarks>
	public abstract class ReconfigurableBase : Configured, Reconfigurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Conf.ReconfigurableBase
			));

		private ReconfigurationUtil reconfigurationUtil = new ReconfigurationUtil();

		/// <summary>Background thread to reload configuration.</summary>
		private Sharpen.Thread reconfigThread = null;

		private volatile bool shouldRun = true;

		private object reconfigLock = new object();

		/// <summary>The timestamp when the <code>reconfigThread</code> starts.</summary>
		private long startTime = 0;

		/// <summary>The timestamp when the <code>reconfigThread</code> finishes.</summary>
		private long endTime = 0;

		/// <summary>A map of <changed property, error message>.</summary>
		/// <remarks>
		/// A map of <changed property, error message>. If error message is present,
		/// it contains the messages about the error occurred when applies the particular
		/// change. Otherwise, it indicates that the change has been successfully applied.
		/// </remarks>
		private IDictionary<ReconfigurationUtil.PropertyChange, Optional<string>> status = 
			null;

		/// <summary>Construct a ReconfigurableBase.</summary>
		public ReconfigurableBase()
			: base(new Configuration())
		{
		}

		/// <summary>
		/// Construct a ReconfigurableBase with the
		/// <see cref="Configuration"/>
		/// conf.
		/// </summary>
		public ReconfigurableBase(Configuration conf)
			: base((conf == null) ? new Configuration() : conf)
		{
		}

		// Use for testing purpose.
		[VisibleForTesting]
		public virtual void SetReconfigurationUtil(ReconfigurationUtil ru)
		{
			reconfigurationUtil = Preconditions.CheckNotNull(ru);
		}

		[VisibleForTesting]
		public virtual ICollection<ReconfigurationUtil.PropertyChange> GetChangedProperties
			(Configuration newConf, Configuration oldConf)
		{
			return reconfigurationUtil.ParseChangedProperties(newConf, oldConf);
		}

		/// <summary>A background thread to apply configuration changes.</summary>
		private class ReconfigurationThread : Sharpen.Thread
		{
			private ReconfigurableBase parent;

			internal ReconfigurationThread(ReconfigurableBase @base)
			{
				this.parent = @base;
			}

			// See {@link ReconfigurationServlet#applyChanges}
			public override void Run()
			{
				Log.Info("Starting reconfiguration task.");
				Configuration oldConf = this.parent.GetConf();
				Configuration newConf = new Configuration();
				ICollection<ReconfigurationUtil.PropertyChange> changes = this.parent.GetChangedProperties
					(newConf, oldConf);
				IDictionary<ReconfigurationUtil.PropertyChange, Optional<string>> results = Maps.
					NewHashMap();
				foreach (ReconfigurationUtil.PropertyChange change in changes)
				{
					string errorMessage = null;
					if (!this.parent.IsPropertyReconfigurable(change.prop))
					{
						errorMessage = "Property " + change.prop + " is not reconfigurable";
						Log.Info(errorMessage);
						results[change] = Optional.Of(errorMessage);
						continue;
					}
					Log.Info("Change property: " + change.prop + " from \"" + ((change.oldVal == null
						) ? "<default>" : change.oldVal) + "\" to \"" + ((change.newVal == null) ? "<default>"
						 : change.newVal) + "\".");
					try
					{
						this.parent.ReconfigurePropertyImpl(change.prop, change.newVal);
					}
					catch (ReconfigurationException e)
					{
						errorMessage = e.InnerException.Message;
					}
					results[change] = Optional.FromNullable(errorMessage);
				}
				lock (this.parent.reconfigLock)
				{
					this.parent.endTime = Time.Now();
					this.parent.status = Sharpen.Collections.UnmodifiableMap(results);
					this.parent.reconfigThread = null;
				}
			}
		}

		/// <summary>Start a reconfiguration task to reload configuration in background.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartReconfigurationTask()
		{
			lock (reconfigLock)
			{
				if (!shouldRun)
				{
					string errorMessage = "The server is stopped.";
					Log.Warn(errorMessage);
					throw new IOException(errorMessage);
				}
				if (reconfigThread != null)
				{
					string errorMessage = "Another reconfiguration task is running.";
					Log.Warn(errorMessage);
					throw new IOException(errorMessage);
				}
				reconfigThread = new ReconfigurableBase.ReconfigurationThread(this);
				reconfigThread.SetDaemon(true);
				reconfigThread.SetName("Reconfiguration Task");
				reconfigThread.Start();
				startTime = Time.Now();
			}
		}

		public virtual ReconfigurationTaskStatus GetReconfigurationTaskStatus()
		{
			lock (reconfigLock)
			{
				if (reconfigThread != null)
				{
					return new ReconfigurationTaskStatus(startTime, 0, null);
				}
				return new ReconfigurationTaskStatus(startTime, endTime, status);
			}
		}

		public virtual void ShutdownReconfigurationTask()
		{
			Sharpen.Thread tempThread;
			lock (reconfigLock)
			{
				shouldRun = false;
				if (reconfigThread == null)
				{
					return;
				}
				tempThread = reconfigThread;
				reconfigThread = null;
			}
			try
			{
				tempThread.Join();
			}
			catch (Exception)
			{
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// This method makes the change to this objects
		/// <see cref="Configuration"/>
		/// and calls reconfigurePropertyImpl to update internal data structures.
		/// This method cannot be overridden, subclasses should instead override
		/// reconfigureProperty.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public string ReconfigureProperty(string property, string newVal)
		{
			if (IsPropertyReconfigurable(property))
			{
				Log.Info("changing property " + property + " to " + newVal);
				string oldVal;
				lock (GetConf())
				{
					oldVal = GetConf().Get(property);
					ReconfigurePropertyImpl(property, newVal);
					if (newVal != null)
					{
						GetConf().Set(property, newVal);
					}
					else
					{
						GetConf().Unset(property);
					}
				}
				return oldVal;
			}
			else
			{
				throw new ReconfigurationException(property, newVal, GetConf().Get(property));
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// Subclasses must override this.
		/// </summary>
		public abstract ICollection<string> GetReconfigurableProperties();

		/// <summary>
		/// <inheritDoc/>
		/// Subclasses may wish to override this with a more efficient implementation.
		/// </summary>
		public virtual bool IsPropertyReconfigurable(string property)
		{
			return GetReconfigurableProperties().Contains(property);
		}

		/// <summary>Change a configuration property.</summary>
		/// <remarks>
		/// Change a configuration property.
		/// Subclasses must override this. This method applies the change to
		/// all internal data structures derived from the configuration property
		/// that is being changed. If this object owns other Reconfigurable objects
		/// reconfigureProperty should be called recursively to make sure that
		/// to make sure that the configuration of these objects is updated.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		protected internal abstract void ReconfigurePropertyImpl(string property, string 
			newVal);
	}
}
