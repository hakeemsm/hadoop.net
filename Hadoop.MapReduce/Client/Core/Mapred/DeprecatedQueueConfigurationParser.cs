using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Class to build queue hierarchy using deprecated conf(mapred-site.xml).</summary>
	/// <remarks>
	/// Class to build queue hierarchy using deprecated conf(mapred-site.xml).
	/// Generates a single level of queue hierarchy.
	/// </remarks>
	internal class DeprecatedQueueConfigurationParser : QueueConfigurationParser
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.DeprecatedQueueConfigurationParser
			));

		internal const string MapredQueueNamesKey = "mapred.queue.names";

		internal DeprecatedQueueConfigurationParser(Configuration conf)
		{
			//If not configuration done return immediately.
			if (!DeprecatedConf(conf))
			{
				return;
			}
			IList<Queue> listq = CreateQueues(conf);
			this.SetAclsEnabled(conf.GetBoolean(MRConfig.MrAclsEnabled, false));
			root = new Queue();
			root.SetName(string.Empty);
			foreach (Queue q in listq)
			{
				root.AddChild(q);
			}
		}

		private IList<Queue> CreateQueues(Configuration conf)
		{
			string[] queueNameValues = conf.GetStrings(MapredQueueNamesKey);
			IList<Queue> list = new AList<Queue>();
			foreach (string name in queueNameValues)
			{
				try
				{
					IDictionary<string, AccessControlList> acls = GetQueueAcls(name, conf);
					QueueState state = GetQueueState(name, conf);
					Queue q = new Queue(name, acls, state);
					list.AddItem(q);
				}
				catch
				{
					Log.Warn("Not able to initialize queue " + name);
				}
			}
			return list;
		}

		/// <summary>
		/// Only applicable to leaf level queues
		/// Parse ACLs for the queue from the configuration.
		/// </summary>
		private QueueState GetQueueState(string name, Configuration conf)
		{
			string stateVal = conf.Get(QueueManager.ToFullPropertyName(name, "state"), QueueState
				.Running.GetStateName());
			return QueueState.GetState(stateVal);
		}

		/// <summary>
		/// Check if queue properties are configured in the passed in
		/// configuration.
		/// </summary>
		/// <remarks>
		/// Check if queue properties are configured in the passed in
		/// configuration. If yes, print out deprecation warning messages.
		/// </remarks>
		private bool DeprecatedConf(Configuration conf)
		{
			string[] queues = null;
			string queueNameValues = GetQueueNames(conf);
			if (queueNameValues == null)
			{
				return false;
			}
			else
			{
				Log.Warn("Configuring \"" + MapredQueueNamesKey + "\" in mapred-site.xml or " + "hadoop-site.xml is deprecated and will overshadow "
					 + QueueConfFileName + ". Remove this property and configure " + "queue hierarchy in "
					 + QueueConfFileName);
				// store queues so we can check if ACLs are also configured
				// in the deprecated files.
				queues = conf.GetStrings(MapredQueueNamesKey);
			}
			// check if acls are defined
			if (queues != null)
			{
				foreach (string queue in queues)
				{
					foreach (QueueACL qAcl in QueueACL.Values())
					{
						string key = QueueManager.ToFullPropertyName(queue, qAcl.GetAclName());
						string aclString = conf.Get(key);
						if (aclString != null)
						{
							Log.Warn("Configuring queue ACLs in mapred-site.xml or " + "hadoop-site.xml is deprecated. Configure queue ACLs in "
								 + QueueConfFileName);
							// even if one string is configured, it is enough for printing
							// the warning. so we can return from here.
							return true;
						}
					}
				}
			}
			return true;
		}

		private string GetQueueNames(Configuration conf)
		{
			string queueNameValues = conf.Get(MapredQueueNamesKey);
			return queueNameValues;
		}

		/// <summary>Parse ACLs for the queue from the configuration.</summary>
		private IDictionary<string, AccessControlList> GetQueueAcls(string name, Configuration
			 conf)
		{
			Dictionary<string, AccessControlList> map = new Dictionary<string, AccessControlList
				>();
			foreach (QueueACL qAcl in QueueACL.Values())
			{
				string aclKey = QueueManager.ToFullPropertyName(name, qAcl.GetAclName());
				map[aclKey] = new AccessControlList(conf.Get(aclKey, "*"));
			}
			return map;
		}
	}
}
