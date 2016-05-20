using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>Utility base class for implementing the Reconfigurable interface.</summary>
	/// <remarks>
	/// Utility base class for implementing the Reconfigurable interface.
	/// Subclasses should override reconfigurePropertyImpl to change individual
	/// properties and getReconfigurableProperties to get all properties that
	/// can be changed at run time.
	/// </remarks>
	public abstract class ReconfigurableBase : org.apache.hadoop.conf.Configured, org.apache.hadoop.conf.Reconfigurable
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.ReconfigurableBase
			)));

		private org.apache.hadoop.conf.ReconfigurationUtil reconfigurationUtil = new org.apache.hadoop.conf.ReconfigurationUtil
			();

		/// <summary>Background thread to reload configuration.</summary>
		private java.lang.Thread reconfigThread = null;

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
		private System.Collections.Generic.IDictionary<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
			, com.google.common.@base.Optional<string>> status = null;

		/// <summary>Construct a ReconfigurableBase.</summary>
		public ReconfigurableBase()
			: base(new org.apache.hadoop.conf.Configuration())
		{
		}

		/// <summary>
		/// Construct a ReconfigurableBase with the
		/// <see cref="Configuration"/>
		/// conf.
		/// </summary>
		public ReconfigurableBase(org.apache.hadoop.conf.Configuration conf)
			: base((conf == null) ? new org.apache.hadoop.conf.Configuration() : conf)
		{
		}

		// Use for testing purpose.
		[com.google.common.annotations.VisibleForTesting]
		public virtual void setReconfigurationUtil(org.apache.hadoop.conf.ReconfigurationUtil
			 ru)
		{
			reconfigurationUtil = com.google.common.@base.Preconditions.checkNotNull(ru);
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
			> getChangedProperties(org.apache.hadoop.conf.Configuration newConf, org.apache.hadoop.conf.Configuration
			 oldConf)
		{
			return reconfigurationUtil.parseChangedProperties(newConf, oldConf);
		}

		/// <summary>A background thread to apply configuration changes.</summary>
		private class ReconfigurationThread : java.lang.Thread
		{
			private org.apache.hadoop.conf.ReconfigurableBase parent;

			internal ReconfigurationThread(org.apache.hadoop.conf.ReconfigurableBase @base)
			{
				this.parent = @base;
			}

			// See {@link ReconfigurationServlet#applyChanges}
			public override void run()
			{
				LOG.info("Starting reconfiguration task.");
				org.apache.hadoop.conf.Configuration oldConf = this.parent.getConf();
				org.apache.hadoop.conf.Configuration newConf = new org.apache.hadoop.conf.Configuration
					();
				System.Collections.Generic.ICollection<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
					> changes = this.parent.getChangedProperties(newConf, oldConf);
				System.Collections.Generic.IDictionary<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
					, com.google.common.@base.Optional<string>> results = com.google.common.collect.Maps
					.newHashMap();
				foreach (org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange change in changes)
				{
					string errorMessage = null;
					if (!this.parent.isPropertyReconfigurable(change.prop))
					{
						errorMessage = "Property " + change.prop + " is not reconfigurable";
						LOG.info(errorMessage);
						results[change] = com.google.common.@base.Optional.of(errorMessage);
						continue;
					}
					LOG.info("Change property: " + change.prop + " from \"" + ((change.oldVal == null
						) ? "<default>" : change.oldVal) + "\" to \"" + ((change.newVal == null) ? "<default>"
						 : change.newVal) + "\".");
					try
					{
						this.parent.reconfigurePropertyImpl(change.prop, change.newVal);
					}
					catch (org.apache.hadoop.conf.ReconfigurationException e)
					{
						errorMessage = e.InnerException.Message;
					}
					results[change] = com.google.common.@base.Optional.fromNullable(errorMessage);
				}
				lock (this.parent.reconfigLock)
				{
					this.parent.endTime = org.apache.hadoop.util.Time.now();
					this.parent.status = java.util.Collections.unmodifiableMap(results);
					this.parent.reconfigThread = null;
				}
			}
		}

		/// <summary>Start a reconfiguration task to reload configuration in background.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void startReconfigurationTask()
		{
			lock (reconfigLock)
			{
				if (!shouldRun)
				{
					string errorMessage = "The server is stopped.";
					LOG.warn(errorMessage);
					throw new System.IO.IOException(errorMessage);
				}
				if (reconfigThread != null)
				{
					string errorMessage = "Another reconfiguration task is running.";
					LOG.warn(errorMessage);
					throw new System.IO.IOException(errorMessage);
				}
				reconfigThread = new org.apache.hadoop.conf.ReconfigurableBase.ReconfigurationThread
					(this);
				reconfigThread.setDaemon(true);
				reconfigThread.setName("Reconfiguration Task");
				reconfigThread.start();
				startTime = org.apache.hadoop.util.Time.now();
			}
		}

		public virtual org.apache.hadoop.conf.ReconfigurationTaskStatus getReconfigurationTaskStatus
			()
		{
			lock (reconfigLock)
			{
				if (reconfigThread != null)
				{
					return new org.apache.hadoop.conf.ReconfigurationTaskStatus(startTime, 0, null);
				}
				return new org.apache.hadoop.conf.ReconfigurationTaskStatus(startTime, endTime, status
					);
			}
		}

		public virtual void shutdownReconfigurationTask()
		{
			java.lang.Thread tempThread;
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
				tempThread.join();
			}
			catch (System.Exception)
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
		/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
		public string reconfigureProperty(string property, string newVal)
		{
			if (isPropertyReconfigurable(property))
			{
				LOG.info("changing property " + property + " to " + newVal);
				string oldVal;
				lock (getConf())
				{
					oldVal = getConf().get(property);
					reconfigurePropertyImpl(property, newVal);
					if (newVal != null)
					{
						getConf().set(property, newVal);
					}
					else
					{
						getConf().unset(property);
					}
				}
				return oldVal;
			}
			else
			{
				throw new org.apache.hadoop.conf.ReconfigurationException(property, newVal, getConf
					().get(property));
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// Subclasses must override this.
		/// </summary>
		public abstract System.Collections.Generic.ICollection<string> getReconfigurableProperties
			();

		/// <summary>
		/// <inheritDoc/>
		/// Subclasses may wish to override this with a more efficient implementation.
		/// </summary>
		public virtual bool isPropertyReconfigurable(string property)
		{
			return getReconfigurableProperties().contains(property);
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
		/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
		protected internal abstract void reconfigurePropertyImpl(string property, string 
			newVal);
	}
}
