using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Class that exposes information about queues maintained by the Hadoop
	/// Map/Reduce framework.
	/// </summary>
	/// <remarks>
	/// Class that exposes information about queues maintained by the Hadoop
	/// Map/Reduce framework.
	/// <p>
	/// The Map/Reduce framework can be configured with one or more queues,
	/// depending on the scheduler it is configured with. While some
	/// schedulers work only with one queue, some schedulers support multiple
	/// queues. Some schedulers also support the notion of queues within
	/// queues - a feature called hierarchical queues.
	/// <p>
	/// Queue names are unique, and used as a key to lookup queues. Hierarchical
	/// queues are named by a 'fully qualified name' such as q1:q2:q3, where
	/// q2 is a child queue of q1 and q3 is a child queue of q2.
	/// <p>
	/// Leaf level queues are queues that contain no queues within them. Jobs
	/// can be submitted only to leaf level queues.
	/// <p>
	/// Queues can be configured with various properties. Some of these
	/// properties are common to all schedulers, and those are handled by this
	/// class. Schedulers might also associate several custom properties with
	/// queues. These properties are parsed and maintained per queue by the
	/// framework. If schedulers need more complicated structure to maintain
	/// configuration per queue, they are free to not use the facilities
	/// provided by the framework, but define their own mechanisms. In such cases,
	/// it is likely that the name of the queue will be used to relate the
	/// common properties of a queue with scheduler specific properties.
	/// <p>
	/// Information related to a queue, such as its name, properties, scheduling
	/// information and children are exposed by this class via a serializable
	/// class called
	/// <see cref="JobQueueInfo"/>
	/// .
	/// <p>
	/// Queues are configured in the configuration file mapred-queues.xml.
	/// To support backwards compatibility, queues can also be configured
	/// in mapred-site.xml. However, when configured in the latter, there is
	/// no support for hierarchical queues.
	/// </remarks>
	public class QueueManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.QueueManager
			));

		private IDictionary<string, Queue> leafQueues = new Dictionary<string, Queue>();

		private IDictionary<string, Queue> allQueues = new Dictionary<string, Queue>();

		public const string QueueConfFileName = "mapred-queues.xml";

		internal const string QueueConfDefaultFileName = "mapred-queues-default.xml";

		internal const string QueueConfPropertyNamePrefix = "mapred.queue.";

		private Queue root = null;

		private bool areAclsEnabled = false;

		// Map of a queue name and Queue object
		//Prefix in configuration for queue related keys
		//Resource in which queue acls are configured.
		// represents if job and queue acls are enabled on the mapreduce cluster
		/// <summary>
		/// Factory method to create an appropriate instance of a queue
		/// configuration parser.
		/// </summary>
		/// <remarks>
		/// Factory method to create an appropriate instance of a queue
		/// configuration parser.
		/// <p>
		/// Returns a parser that can parse either the deprecated property
		/// style queue configuration in mapred-site.xml, or one that can
		/// parse hierarchical queues in mapred-queues.xml. First preference
		/// is given to configuration in mapred-site.xml. If no queue
		/// configuration is found there, then a parser that can parse
		/// configuration in mapred-queues.xml is created.
		/// </remarks>
		/// <param name="conf">
		/// Configuration instance that determines which parser
		/// to use.
		/// </param>
		/// <returns>Queue configuration parser</returns>
		internal static QueueConfigurationParser GetQueueConfigurationParser(Configuration
			 conf, bool reloadConf, bool areAclsEnabled)
		{
			if (conf != null && conf.Get(DeprecatedQueueConfigurationParser.MapredQueueNamesKey
				) != null)
			{
				if (reloadConf)
				{
					conf.ReloadConfiguration();
				}
				return new DeprecatedQueueConfigurationParser(conf);
			}
			else
			{
				Uri xmlInUrl = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource
					(QueueConfFileName);
				if (xmlInUrl == null)
				{
					xmlInUrl = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource(QueueConfDefaultFileName
						);
					System.Diagnostics.Debug.Assert(xmlInUrl != null);
				}
				// this should be in our jar
				InputStream stream = null;
				try
				{
					stream = xmlInUrl.OpenStream();
					return new QueueConfigurationParser(new BufferedInputStream(stream), areAclsEnabled
						);
				}
				catch (IOException ioe)
				{
					throw new RuntimeException("Couldn't open queue configuration at " + xmlInUrl, ioe
						);
				}
				finally
				{
					IOUtils.CloseStream(stream);
				}
			}
		}

		internal QueueManager()
			: this(false)
		{
		}

		internal QueueManager(bool areAclsEnabled)
		{
			// acls are disabled
			this.areAclsEnabled = areAclsEnabled;
			Initialize(GetQueueConfigurationParser(null, false, areAclsEnabled));
		}

		/// <summary>
		/// Construct a new QueueManager using configuration specified in the passed
		/// in
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// object.
		/// <p>
		/// This instance supports queue configuration specified in mapred-site.xml,
		/// but without support for hierarchical queues. If no queue configuration
		/// is found in mapred-site.xml, it will then look for site configuration
		/// in mapred-queues.xml supporting hierarchical queues.
		/// </summary>
		/// <param name="clusterConf">mapreduce cluster configuration</param>
		public QueueManager(Configuration clusterConf)
		{
			areAclsEnabled = clusterConf.GetBoolean(MRConfig.MrAclsEnabled, false);
			Initialize(GetQueueConfigurationParser(clusterConf, false, areAclsEnabled));
		}

		/// <summary>
		/// Create an instance that supports hierarchical queues, defined in
		/// the passed in configuration file.
		/// </summary>
		/// <remarks>
		/// Create an instance that supports hierarchical queues, defined in
		/// the passed in configuration file.
		/// <p>
		/// This is mainly used for testing purposes and should not called from
		/// production code.
		/// </remarks>
		/// <param name="confFile">File where the queue configuration is found.</param>
		internal QueueManager(string confFile, bool areAclsEnabled)
		{
			this.areAclsEnabled = areAclsEnabled;
			QueueConfigurationParser cp = new QueueConfigurationParser(confFile, areAclsEnabled
				);
			Initialize(cp);
		}

		/// <summary>
		/// Initialize the queue-manager with the queue hierarchy specified by the
		/// given
		/// <see cref="QueueConfigurationParser"/>
		/// .
		/// </summary>
		/// <param name="cp"/>
		private void Initialize(QueueConfigurationParser cp)
		{
			this.root = cp.GetRoot();
			leafQueues.Clear();
			allQueues.Clear();
			//At this point we have root populated
			//update data structures leafNodes.
			leafQueues = GetRoot().GetLeafQueues();
			allQueues.PutAll(GetRoot().GetInnerQueues());
			allQueues.PutAll(leafQueues);
			Log.Info("AllQueues : " + allQueues + "; LeafQueues : " + leafQueues);
		}

		/// <summary>
		/// Return the set of leaf level queues configured in the system to
		/// which jobs are submitted.
		/// </summary>
		/// <remarks>
		/// Return the set of leaf level queues configured in the system to
		/// which jobs are submitted.
		/// <p>
		/// The number of queues configured should be dependent on the Scheduler
		/// configured. Note that some schedulers work with only one queue, whereas
		/// others can support multiple queues.
		/// </remarks>
		/// <returns>Set of queue names.</returns>
		public virtual ICollection<string> GetLeafQueueNames()
		{
			lock (this)
			{
				return leafQueues.Keys;
			}
		}

		/// <summary>
		/// Return true if the given user is part of the ACL for the given
		/// <see cref="QueueACL"/>
		/// name for the given queue.
		/// <p>
		/// An operation is allowed if all users are provided access for this
		/// operation, or if either the user or any of the groups specified is
		/// provided access.
		/// </summary>
		/// <param name="queueName">Queue on which the operation needs to be performed.</param>
		/// <param name="qACL">The queue ACL name to be checked</param>
		/// <param name="ugi">The user and groups who wish to perform the operation.</param>
		/// <returns>true     if the operation is allowed, false otherwise.</returns>
		public virtual bool HasAccess(string queueName, QueueACL qACL, UserGroupInformation
			 ugi)
		{
			lock (this)
			{
				Queue q = leafQueues[queueName];
				if (q == null)
				{
					Log.Info("Queue " + queueName + " is not present");
					return false;
				}
				if (q.GetChildren() != null && !q.GetChildren().IsEmpty())
				{
					Log.Info("Cannot submit job to parent queue " + q.GetName());
					return false;
				}
				if (!AreAclsEnabled())
				{
					return true;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Checking access for the acl " + ToFullPropertyName(queueName, qACL.GetAclName
						()) + " for user " + ugi.GetShortUserName());
				}
				AccessControlList acl = q.GetAcls()[ToFullPropertyName(queueName, qACL.GetAclName
					())];
				if (acl == null)
				{
					return false;
				}
				// Check if user is part of the ACL
				return acl.IsUserAllowed(ugi);
			}
		}

		/// <summary>Checks whether the given queue is running or not.</summary>
		/// <param name="queueName">name of the queue</param>
		/// <returns>true, if the queue is running.</returns>
		internal virtual bool IsRunning(string queueName)
		{
			lock (this)
			{
				Queue q = leafQueues[queueName];
				if (q != null)
				{
					return q.GetState().Equals(QueueState.Running);
				}
				return false;
			}
		}

		/// <summary>
		/// Set a generic Object that represents scheduling information relevant
		/// to a queue.
		/// </summary>
		/// <remarks>
		/// Set a generic Object that represents scheduling information relevant
		/// to a queue.
		/// <p>
		/// A string representation of this Object will be used by the framework
		/// to display in user facing applications like the JobTracker web UI and
		/// the hadoop CLI.
		/// </remarks>
		/// <param name="queueName">queue for which the scheduling information is to be set.</param>
		/// <param name="queueInfo">scheduling information for this queue.</param>
		public virtual void SetSchedulerInfo(string queueName, object queueInfo)
		{
			lock (this)
			{
				if (allQueues[queueName] != null)
				{
					allQueues[queueName].SetSchedulingInfo(queueInfo);
				}
			}
		}

		/// <summary>Return the scheduler information configured for this queue.</summary>
		/// <param name="queueName">queue for which the scheduling information is required.</param>
		/// <returns>The scheduling information for this queue.</returns>
		public virtual object GetSchedulerInfo(string queueName)
		{
			lock (this)
			{
				if (allQueues[queueName] != null)
				{
					return allQueues[queueName].GetSchedulingInfo();
				}
				return null;
			}
		}

		internal const string MsgRefreshFailureWithChangeOfHierarchy = "Unable to refresh queues because queue-hierarchy changed. "
			 + "Retaining existing configuration. ";

		internal const string MsgRefreshFailureWithSchedulerFailure = "Scheduler couldn't refresh it's queues with the new"
			 + " configuration properties. " + "Retaining existing configuration throughout the system.";

		/// <summary>Refresh acls, state and scheduler properties for the configured queues.</summary>
		/// <remarks>
		/// Refresh acls, state and scheduler properties for the configured queues.
		/// <p>
		/// This method reloads configuration related to queues, but does not
		/// support changes to the list of queues or hierarchy. The expected usage
		/// is that an administrator can modify the queue configuration file and
		/// fire an admin command to reload queue configuration. If there is a
		/// problem in reloading configuration, then this method guarantees that
		/// existing queue configuration is untouched and in a consistent state.
		/// </remarks>
		/// <param name="schedulerRefresher"/>
		/// <exception cref="System.IO.IOException">when queue configuration file is invalid.
		/// 	</exception>
		internal virtual void RefreshQueues(Configuration conf, QueueRefresher schedulerRefresher
			)
		{
			lock (this)
			{
				// Create a new configuration parser using the passed conf object.
				QueueConfigurationParser cp = GetQueueConfigurationParser(conf, true, areAclsEnabled
					);
				/*
				* (1) Validate the refresh of properties owned by QueueManager. As of now,
				* while refreshing queue properties, we only check that the hierarchy is
				* the same w.r.t queue names, ACLs and state for each queue and don't
				* support adding new queues or removing old queues
				*/
				if (!root.IsHierarchySameAs(cp.GetRoot()))
				{
					Log.Warn(MsgRefreshFailureWithChangeOfHierarchy);
					throw new IOException(MsgRefreshFailureWithChangeOfHierarchy);
				}
				/*
				* (2) QueueManager owned properties are validated. Now validate and
				* refresh the properties of scheduler in a single step.
				*/
				if (schedulerRefresher != null)
				{
					try
					{
						schedulerRefresher.RefreshQueues(cp.GetRoot().GetJobQueueInfo().GetChildren());
					}
					catch (Exception e)
					{
						StringBuilder msg = new StringBuilder("Scheduler's refresh-queues failed with the exception : "
							 + StringUtils.StringifyException(e));
						msg.Append("\n");
						msg.Append(MsgRefreshFailureWithSchedulerFailure);
						Log.Error(msg.ToString());
						throw new IOException(msg.ToString());
					}
				}
				/*
				* (3) Scheduler has validated and refreshed its queues successfully, now
				* refresh the properties owned by QueueManager
				*/
				// First copy the scheduling information recursively into the new
				// queue-hierarchy. This is done to retain old scheduling information. This
				// is done after scheduler refresh and not before it because during refresh,
				// schedulers may wish to change their scheduling info objects too.
				cp.GetRoot().CopySchedulingInfo(this.root);
				// Now switch roots.
				Initialize(cp);
				Log.Info("Queue configuration is refreshed successfully.");
			}
		}

		// this method is for internal use only
		public static string ToFullPropertyName(string queue, string property)
		{
			return QueueConfPropertyNamePrefix + queue + "." + property;
		}

		/// <summary>
		/// Return an array of
		/// <see cref="JobQueueInfo"/>
		/// objects for all the
		/// queues configurated in the system.
		/// </summary>
		/// <returns>array of JobQueueInfo objects.</returns>
		internal virtual JobQueueInfo[] GetJobQueueInfos()
		{
			lock (this)
			{
				AList<JobQueueInfo> queueInfoList = new AList<JobQueueInfo>();
				foreach (string queue in allQueues.Keys)
				{
					JobQueueInfo queueInfo = GetJobQueueInfo(queue);
					if (queueInfo != null)
					{
						queueInfoList.AddItem(queueInfo);
					}
				}
				return Sharpen.Collections.ToArray(queueInfoList, new JobQueueInfo[queueInfoList.
					Count]);
			}
		}

		/// <summary>
		/// Return
		/// <see cref="JobQueueInfo"/>
		/// for a given queue.
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>JobQueueInfo for the queue, null if the queue is not found.</returns>
		internal virtual JobQueueInfo GetJobQueueInfo(string queue)
		{
			lock (this)
			{
				if (allQueues.Contains(queue))
				{
					return allQueues[queue].GetJobQueueInfo();
				}
				return null;
			}
		}

		/// <summary>JobQueueInfo for all the queues.</summary>
		/// <remarks>
		/// JobQueueInfo for all the queues.
		/// <p>
		/// Contribs can use this data structure to either create a hierarchy or for
		/// traversing.
		/// They can also use this to refresh properties in case of refreshQueues
		/// </remarks>
		/// <returns>a map for easy navigation.</returns>
		internal virtual IDictionary<string, JobQueueInfo> GetJobQueueInfoMapping()
		{
			lock (this)
			{
				IDictionary<string, JobQueueInfo> m = new Dictionary<string, JobQueueInfo>();
				foreach (KeyValuePair<string, Queue> entry in allQueues)
				{
					m[entry.Key] = entry.Value.GetJobQueueInfo();
				}
				return m;
			}
		}

		/// <summary>Generates the array of QueueAclsInfo object.</summary>
		/// <remarks>
		/// Generates the array of QueueAclsInfo object.
		/// <p>
		/// The array consists of only those queues for which user has acls.
		/// </remarks>
		/// <returns>QueueAclsInfo[]</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual QueueAclsInfo[] GetQueueAcls(UserGroupInformation ugi)
		{
			lock (this)
			{
				//List of all QueueAclsInfo objects , this list is returned
				AList<QueueAclsInfo> queueAclsInfolist = new AList<QueueAclsInfo>();
				QueueACL[] qAcls = QueueACL.Values();
				foreach (string queueName in leafQueues.Keys)
				{
					QueueAclsInfo queueAclsInfo = null;
					AList<string> operationsAllowed = null;
					foreach (QueueACL qAcl in qAcls)
					{
						if (HasAccess(queueName, qAcl, ugi))
						{
							if (operationsAllowed == null)
							{
								operationsAllowed = new AList<string>();
							}
							operationsAllowed.AddItem(qAcl.GetAclName());
						}
					}
					if (operationsAllowed != null)
					{
						//There is atleast 1 operation supported for queue <queueName>
						//, hence initialize queueAclsInfo
						queueAclsInfo = new QueueAclsInfo(queueName, Sharpen.Collections.ToArray(operationsAllowed
							, new string[operationsAllowed.Count]));
						queueAclsInfolist.AddItem(queueAclsInfo);
					}
				}
				return Sharpen.Collections.ToArray(queueAclsInfolist, new QueueAclsInfo[queueAclsInfolist
					.Count]);
			}
		}

		/// <summary>Return if ACLs are enabled for the Map/Reduce system</summary>
		/// <returns>true if ACLs are enabled.</returns>
		internal virtual bool AreAclsEnabled()
		{
			return areAclsEnabled;
		}

		/// <summary>Used only for test.</summary>
		/// <returns/>
		internal virtual Queue GetRoot()
		{
			return root;
		}

		/// <summary>Dumps the configuration of hierarchy of queues</summary>
		/// <param name="out">the writer object to which dump is written</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void DumpConfiguration(TextWriter @out, Configuration conf)
		{
			DumpConfiguration(@out, null, conf);
		}

		/// <summary>
		/// Dumps the configuration of hierarchy of queues with
		/// the xml file path given.
		/// </summary>
		/// <remarks>
		/// Dumps the configuration of hierarchy of queues with
		/// the xml file path given. It is to be used directly ONLY FOR TESTING.
		/// </remarks>
		/// <param name="out">the writer object to which dump is written to.</param>
		/// <param name="configFile">the filename of xml file</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void DumpConfiguration(TextWriter @out, string configFile, Configuration
			 conf)
		{
			if (conf != null && conf.Get(DeprecatedQueueConfigurationParser.MapredQueueNamesKey
				) != null)
			{
				return;
			}
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.CreateJsonGenerator(@out);
			QueueConfigurationParser parser;
			bool aclsEnabled = false;
			if (conf != null)
			{
				aclsEnabled = conf.GetBoolean(MRConfig.MrAclsEnabled, false);
			}
			if (configFile != null && !string.Empty.Equals(configFile))
			{
				parser = new QueueConfigurationParser(configFile, aclsEnabled);
			}
			else
			{
				parser = GetQueueConfigurationParser(null, false, aclsEnabled);
			}
			dumpGenerator.WriteStartObject();
			dumpGenerator.WriteFieldName("queues");
			dumpGenerator.WriteStartArray();
			DumpConfiguration(dumpGenerator, parser.GetRoot().GetChildren());
			dumpGenerator.WriteEndArray();
			dumpGenerator.WriteEndObject();
			dumpGenerator.Flush();
		}

		/// <summary>
		/// method to perform depth-first search and write the parameters of every
		/// queue in JSON format.
		/// </summary>
		/// <param name="dumpGenerator">
		/// JsonGenerator object which takes the dump and flushes
		/// to a writer object
		/// </param>
		/// <param name="rootQueues">the top-level queues</param>
		/// <exception cref="Org.Codehaus.Jackson.JsonGenerationException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void DumpConfiguration(JsonGenerator dumpGenerator, ICollection<Queue
			> rootQueues)
		{
			foreach (Queue queue in rootQueues)
			{
				dumpGenerator.WriteStartObject();
				dumpGenerator.WriteStringField("name", queue.GetName());
				dumpGenerator.WriteStringField("state", queue.GetState().ToString());
				AccessControlList submitJobList = null;
				AccessControlList administerJobsList = null;
				if (queue.GetAcls() != null)
				{
					submitJobList = queue.GetAcls()[ToFullPropertyName(queue.GetName(), QueueACL.SubmitJob
						.GetAclName())];
					administerJobsList = queue.GetAcls()[ToFullPropertyName(queue.GetName(), QueueACL
						.AdministerJobs.GetAclName())];
				}
				string aclsSubmitJobValue = " ";
				if (submitJobList != null)
				{
					aclsSubmitJobValue = submitJobList.GetAclString();
				}
				dumpGenerator.WriteStringField("acl_submit_job", aclsSubmitJobValue);
				string aclsAdministerValue = " ";
				if (administerJobsList != null)
				{
					aclsAdministerValue = administerJobsList.GetAclString();
				}
				dumpGenerator.WriteStringField("acl_administer_jobs", aclsAdministerValue);
				dumpGenerator.WriteFieldName("properties");
				dumpGenerator.WriteStartArray();
				if (queue.GetProperties() != null)
				{
					foreach (KeyValuePair<object, object> property in queue.GetProperties())
					{
						dumpGenerator.WriteStartObject();
						dumpGenerator.WriteStringField("key", (string)property.Key);
						dumpGenerator.WriteStringField("value", (string)property.Value);
						dumpGenerator.WriteEndObject();
					}
				}
				dumpGenerator.WriteEndArray();
				ICollection<Queue> childQueues = queue.GetChildren();
				dumpGenerator.WriteFieldName("children");
				dumpGenerator.WriteStartArray();
				if (childQueues != null && childQueues.Count > 0)
				{
					DumpConfiguration(dumpGenerator, childQueues);
				}
				dumpGenerator.WriteEndArray();
				dumpGenerator.WriteEndObject();
			}
		}
	}
}
