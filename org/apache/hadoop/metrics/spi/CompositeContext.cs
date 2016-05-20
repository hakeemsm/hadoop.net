using Sharpen;

namespace org.apache.hadoop.metrics.spi
{
	public class CompositeContext : org.apache.hadoop.metrics.spi.AbstractMetricsContext
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.spi.CompositeContext
			)));

		private const string ARITY_LABEL = "arity";

		private const string SUB_FMT = "%s.sub%d";

		private readonly System.Collections.Generic.List<org.apache.hadoop.metrics.MetricsContext
			> subctxt = new System.Collections.Generic.List<org.apache.hadoop.metrics.MetricsContext
			>();

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public CompositeContext()
		{
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void init(string contextName, org.apache.hadoop.metrics.ContextFactory
			 factory)
		{
			base.init(contextName, factory);
			int nKids;
			try
			{
				string sKids = getAttribute(ARITY_LABEL);
				nKids = System.Convert.ToInt32(sKids);
			}
			catch (System.Exception e)
			{
				LOG.error("Unable to initialize composite metric " + contextName + ": could not init arity"
					, e);
				return;
			}
			for (int i = 0; i < nKids; ++i)
			{
				org.apache.hadoop.metrics.MetricsContext ctxt = org.apache.hadoop.metrics.MetricsUtil
					.getContext(string.format(SUB_FMT, contextName, i), contextName);
				if (null != ctxt)
				{
					subctxt.add(ctxt);
				}
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		protected internal override org.apache.hadoop.metrics.MetricsRecord newRecord(string
			 recordName)
		{
			return (org.apache.hadoop.metrics.MetricsRecord)java.lang.reflect.Proxy.newProxyInstance
				(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.MetricsRecord)
				).getClassLoader(), new java.lang.Class[] { Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.metrics.MetricsRecord)) }, new org.apache.hadoop.metrics.spi.CompositeContext.MetricsRecordDelegator
				(recordName, subctxt));
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		protected internal override void emitRecord(string contextName, string recordName
			, org.apache.hadoop.metrics.spi.OutputRecord outRec)
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				try
				{
					((org.apache.hadoop.metrics.spi.AbstractMetricsContext)ctxt).emitRecord(contextName
						, recordName, outRec);
					if (contextName == null || recordName == null || outRec == null)
					{
						throw new System.IO.IOException(contextName + ":" + recordName + ":" + outRec);
					}
				}
				catch (System.IO.IOException e)
				{
					LOG.warn("emitRecord failed: " + ctxt.getContextName(), e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		protected internal override void flush()
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				try
				{
					((org.apache.hadoop.metrics.spi.AbstractMetricsContext)ctxt).flush();
				}
				catch (System.IO.IOException e)
				{
					LOG.warn("flush failed: " + ctxt.getContextName(), e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void startMonitoring()
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				try
				{
					ctxt.startMonitoring();
				}
				catch (System.IO.IOException e)
				{
					LOG.warn("startMonitoring failed: " + ctxt.getContextName(), e);
				}
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void stopMonitoring()
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				ctxt.stopMonitoring();
			}
		}

		/// <summary>Return true if all subcontexts are monitoring.</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override bool isMonitoring()
		{
			bool ret = true;
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				ret &= ctxt.isMonitoring();
			}
			return ret;
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void close()
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				ctxt.close();
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void registerUpdater(org.apache.hadoop.metrics.Updater updater)
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				ctxt.registerUpdater(updater);
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void unregisterUpdater(org.apache.hadoop.metrics.Updater updater)
		{
			foreach (org.apache.hadoop.metrics.MetricsContext ctxt in subctxt)
			{
				ctxt.unregisterUpdater(updater);
			}
		}

		private class MetricsRecordDelegator : java.lang.reflect.InvocationHandler
		{
			private static readonly java.lang.reflect.Method m_getRecordName = initMethod();

			private static java.lang.reflect.Method initMethod()
			{
				try
				{
					return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.MetricsRecord
						)).getMethod("getRecordName", new java.lang.Class[0]);
				}
				catch (System.Exception e)
				{
					throw new System.Exception("Internal error", e);
				}
			}

			private readonly string recordName;

			private readonly System.Collections.Generic.List<org.apache.hadoop.metrics.MetricsRecord
				> subrecs;

			internal MetricsRecordDelegator(string recordName, System.Collections.Generic.List
				<org.apache.hadoop.metrics.MetricsContext> ctxts)
			{
				this.recordName = recordName;
				this.subrecs = new System.Collections.Generic.List<org.apache.hadoop.metrics.MetricsRecord
					>(ctxts.Count);
				foreach (org.apache.hadoop.metrics.MetricsContext ctxt in ctxts)
				{
					subrecs.add(ctxt.createRecord(recordName));
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual object invoke(object p, java.lang.reflect.Method m, object[] args)
			{
				if (m_getRecordName.Equals(m))
				{
					return recordName;
				}
				System.Diagnostics.Debug.Assert(Sharpen.Runtime.getClassForType(typeof(void)).Equals
					(m.getReturnType()));
				foreach (org.apache.hadoop.metrics.MetricsRecord rec in subrecs)
				{
					m.invoke(rec, args);
				}
				return null;
			}
		}
	}
}
