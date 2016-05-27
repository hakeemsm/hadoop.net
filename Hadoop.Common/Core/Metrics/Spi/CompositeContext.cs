using System;
using System.IO;
using System.Reflection;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Metrics.Spi
{
	public class CompositeContext : AbstractMetricsContext
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics.Spi.CompositeContext
			));

		private const string ArityLabel = "arity";

		private const string SubFmt = "%s.sub%d";

		private readonly AList<MetricsContext> subctxt = new AList<MetricsContext>();

		[InterfaceAudience.Private]
		public CompositeContext()
		{
		}

		[InterfaceAudience.Private]
		public override void Init(string contextName, ContextFactory factory)
		{
			base.Init(contextName, factory);
			int nKids;
			try
			{
				string sKids = GetAttribute(ArityLabel);
				nKids = System.Convert.ToInt32(sKids);
			}
			catch (Exception e)
			{
				Log.Error("Unable to initialize composite metric " + contextName + ": could not init arity"
					, e);
				return;
			}
			for (int i = 0; i < nKids; ++i)
			{
				MetricsContext ctxt = MetricsUtil.GetContext(string.Format(SubFmt, contextName, i
					), contextName);
				if (null != ctxt)
				{
					subctxt.AddItem(ctxt);
				}
			}
		}

		[InterfaceAudience.Private]
		protected internal override MetricsRecord NewRecord(string recordName)
		{
			return (MetricsRecord)Proxy.NewProxyInstance(typeof(MetricsRecord).GetClassLoader
				(), new Type[] { typeof(MetricsRecord) }, new CompositeContext.MetricsRecordDelegator
				(recordName, subctxt));
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected internal override void EmitRecord(string contextName, string recordName
			, OutputRecord outRec)
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				try
				{
					((AbstractMetricsContext)ctxt).EmitRecord(contextName, recordName, outRec);
					if (contextName == null || recordName == null || outRec == null)
					{
						throw new IOException(contextName + ":" + recordName + ":" + outRec);
					}
				}
				catch (IOException e)
				{
					Log.Warn("emitRecord failed: " + ctxt.GetContextName(), e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected internal override void Flush()
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				try
				{
					((AbstractMetricsContext)ctxt).Flush();
				}
				catch (IOException e)
				{
					Log.Warn("flush failed: " + ctxt.GetContextName(), e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public override void StartMonitoring()
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				try
				{
					ctxt.StartMonitoring();
				}
				catch (IOException e)
				{
					Log.Warn("startMonitoring failed: " + ctxt.GetContextName(), e);
				}
			}
		}

		[InterfaceAudience.Private]
		public override void StopMonitoring()
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				ctxt.StopMonitoring();
			}
		}

		/// <summary>Return true if all subcontexts are monitoring.</summary>
		[InterfaceAudience.Private]
		public override bool IsMonitoring()
		{
			bool ret = true;
			foreach (MetricsContext ctxt in subctxt)
			{
				ret &= ctxt.IsMonitoring();
			}
			return ret;
		}

		[InterfaceAudience.Private]
		public override void Close()
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				ctxt.Close();
			}
		}

		[InterfaceAudience.Private]
		public override void RegisterUpdater(Updater updater)
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				ctxt.RegisterUpdater(updater);
			}
		}

		[InterfaceAudience.Private]
		public override void UnregisterUpdater(Updater updater)
		{
			foreach (MetricsContext ctxt in subctxt)
			{
				ctxt.UnregisterUpdater(updater);
			}
		}

		private class MetricsRecordDelegator : InvocationHandler
		{
			private static readonly MethodInfo m_getRecordName = InitMethod();

			private static MethodInfo InitMethod()
			{
				try
				{
					return typeof(MetricsRecord).GetMethod("getRecordName", new Type[0]);
				}
				catch (Exception e)
				{
					throw new RuntimeException("Internal error", e);
				}
			}

			private readonly string recordName;

			private readonly AList<MetricsRecord> subrecs;

			internal MetricsRecordDelegator(string recordName, AList<MetricsContext> ctxts)
			{
				this.recordName = recordName;
				this.subrecs = new AList<MetricsRecord>(ctxts.Count);
				foreach (MetricsContext ctxt in ctxts)
				{
					subrecs.AddItem(ctxt.CreateRecord(recordName));
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual object Invoke(object p, MethodInfo m, object[] args)
			{
				if (m_getRecordName.Equals(m))
				{
					return recordName;
				}
				System.Diagnostics.Debug.Assert(typeof(void).Equals(m.ReturnType));
				foreach (MetricsRecord rec in subrecs)
				{
					m.Invoke(rec, args);
				}
				return null;
			}
		}
	}
}
