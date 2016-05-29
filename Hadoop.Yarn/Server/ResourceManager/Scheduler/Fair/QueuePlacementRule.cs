using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.W3c.Dom;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public abstract class QueuePlacementRule
	{
		protected internal bool create;

		public static readonly Log Log = LogFactory.GetLog(typeof(QueuePlacementRule).FullName
			);

		/// <summary>Initializes the rule with any arguments.</summary>
		/// <param name="args">Additional attributes of the rule's xml element other than create.
		/// 	</param>
		public virtual QueuePlacementRule Initialize(bool create, IDictionary<string, string
			> args)
		{
			this.create = create;
			return this;
		}

		/// <param name="requestedQueue">The queue explicitly requested.</param>
		/// <param name="user">The user submitting the app.</param>
		/// <param name="groups">The groups of the user submitting the app.</param>
		/// <param name="configuredQueues">The queues specified in the scheduler configuration.
		/// 	</param>
		/// <returns>
		/// The queue to place the app into. An empty string indicates that we should
		/// continue to the next rule, and null indicates that the app should be rejected.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string AssignAppToQueue(string requestedQueue, string user, Groups
			 groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
		{
			string queue = GetQueueForApp(requestedQueue, user, groups, configuredQueues);
			if (create || configuredQueues[FSQueueType.Leaf].Contains(queue) || configuredQueues
				[FSQueueType.Parent].Contains(queue))
			{
				return queue;
			}
			else
			{
				return string.Empty;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public virtual void InitializeFromXml(Element el)
		{
			bool create = true;
			NamedNodeMap attributes = el.GetAttributes();
			IDictionary<string, string> args = new Dictionary<string, string>();
			for (int i = 0; i < attributes.GetLength(); i++)
			{
				Node node = attributes.Item(i);
				string key = node.GetNodeName();
				string value = node.GetNodeValue();
				if (key.Equals("create"))
				{
					create = System.Boolean.Parse(value);
				}
				else
				{
					args[key] = value;
				}
			}
			Initialize(create, args);
		}

		/// <summary>Returns true if this rule never tells the policy to continue.</summary>
		public abstract bool IsTerminal();

		/// <summary>
		/// Applies this rule to an app with the given requested queue and user/group
		/// information.
		/// </summary>
		/// <param name="requestedQueue">The queue specified in the ApplicationSubmissionContext
		/// 	</param>
		/// <param name="user">The user submitting the app.</param>
		/// <param name="groups">The groups of the user submitting the app.</param>
		/// <returns>
		/// The name of the queue to assign the app to, or null to empty string
		/// continue to the next rule.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract string GetQueueForApp(string requestedQueue, string user
			, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues);

		/// <summary>Places apps in queues by username of the submitter</summary>
		public class User : QueuePlacementRule
		{
			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				return "root." + CleanName(user);
			}

			public override bool IsTerminal()
			{
				return create;
			}
		}

		/// <summary>Places apps in queues by primary group of the submitter</summary>
		public class PrimaryGroup : QueuePlacementRule
		{
			/// <exception cref="System.IO.IOException"/>
			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				return "root." + CleanName(groups.GetGroups(user)[0]);
			}

			public override bool IsTerminal()
			{
				return create;
			}
		}

		/// <summary>
		/// Places apps in queues by secondary group of the submitter
		/// Match will be made on first secondary group that exist in
		/// queues
		/// </summary>
		public class SecondaryGroupExistingQueue : QueuePlacementRule
		{
			/// <exception cref="System.IO.IOException"/>
			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				IList<string> groupNames = groups.GetGroups(user);
				for (int i = 1; i < groupNames.Count; i++)
				{
					string group = CleanName(groupNames[i]);
					if (configuredQueues[FSQueueType.Leaf].Contains("root." + group) || configuredQueues
						[FSQueueType.Parent].Contains("root." + group))
					{
						return "root." + group;
					}
				}
				return string.Empty;
			}

			public override bool IsTerminal()
			{
				return false;
			}
		}

		/// <summary>
		/// Places apps in queues with name of the submitter under the queue
		/// returned by the nested rule.
		/// </summary>
		public class NestedUserQueue : QueuePlacementRule
		{
			[VisibleForTesting]
			internal QueuePlacementRule nestedRule;

			/// <summary>Parse xml and instantiate the nested rule</summary>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
			/// 	"/>
			public override void InitializeFromXml(Element el)
			{
				NodeList elements = el.GetChildNodes();
				for (int i = 0; i < elements.GetLength(); i++)
				{
					Node node = elements.Item(i);
					if (node is Element)
					{
						Element element = (Element)node;
						if ("rule".Equals(element.GetTagName()))
						{
							QueuePlacementRule rule = QueuePlacementPolicy.CreateAndInitializeRule(node);
							if (rule == null)
							{
								throw new AllocationConfigurationException("Unable to create nested rule in nestedUserQueue rule"
									);
							}
							this.nestedRule = rule;
							break;
						}
						else
						{
							continue;
						}
					}
				}
				if (this.nestedRule == null)
				{
					throw new AllocationConfigurationException("No nested rule specified in <nestedUserQueue> rule"
						);
				}
				base.InitializeFromXml(el);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				// Apply the nested rule
				string queueName = nestedRule.AssignAppToQueue(requestedQueue, user, groups, configuredQueues
					);
				if (queueName != null && queueName.Length != 0)
				{
					if (!queueName.StartsWith("root."))
					{
						queueName = "root." + queueName;
					}
					// Verify if the queue returned by the nested rule is an configured leaf queue,
					// if yes then skip to next rule in the queue placement policy
					if (configuredQueues[FSQueueType.Leaf].Contains(queueName))
					{
						return string.Empty;
					}
					return queueName + "." + CleanName(user);
				}
				return queueName;
			}

			public override bool IsTerminal()
			{
				return false;
			}
		}

		/// <summary>Places apps in queues by requested queue of the submitter</summary>
		public class Specified : QueuePlacementRule
		{
			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				if (requestedQueue.Equals(YarnConfiguration.DefaultQueueName))
				{
					return string.Empty;
				}
				else
				{
					if (!requestedQueue.StartsWith("root."))
					{
						requestedQueue = "root." + requestedQueue;
					}
					return requestedQueue;
				}
			}

			public override bool IsTerminal()
			{
				return false;
			}
		}

		/// <summary>Places apps in the specified default queue.</summary>
		/// <remarks>
		/// Places apps in the specified default queue. If no default queue is
		/// specified the app is placed in root.default queue.
		/// </remarks>
		public class Default : QueuePlacementRule
		{
			[VisibleForTesting]
			internal string defaultQueueName;

			public override QueuePlacementRule Initialize(bool create, IDictionary<string, string
				> args)
			{
				if (defaultQueueName == null)
				{
					defaultQueueName = "root." + YarnConfiguration.DefaultQueueName;
				}
				return base.Initialize(create, args);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
			/// 	"/>
			public override void InitializeFromXml(Element el)
			{
				defaultQueueName = el.GetAttribute("queue");
				if (defaultQueueName != null && !defaultQueueName.IsEmpty())
				{
					if (!defaultQueueName.StartsWith("root."))
					{
						defaultQueueName = "root." + defaultQueueName;
					}
				}
				else
				{
					defaultQueueName = "root." + YarnConfiguration.DefaultQueueName;
				}
				base.InitializeFromXml(el);
			}

			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				return defaultQueueName;
			}

			public override bool IsTerminal()
			{
				return true;
			}
		}

		/// <summary>Rejects all apps</summary>
		public class Reject : QueuePlacementRule
		{
			public override string AssignAppToQueue(string requestedQueue, string user, Groups
				 groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				return null;
			}

			protected internal override string GetQueueForApp(string requestedQueue, string user
				, Groups groups, IDictionary<FSQueueType, ICollection<string>> configuredQueues)
			{
				throw new NotSupportedException();
			}

			public override bool IsTerminal()
			{
				return true;
			}
		}

		/// <summary>Replace the periods in the username or groupname with "_dot_".</summary>
		protected internal virtual string CleanName(string name)
		{
			if (name.Contains("."))
			{
				string converted = name.ReplaceAll("\\.", "_dot_");
				Log.Warn("Name " + name + " is converted to " + converted + " when it is used as a queue name."
					);
				return converted;
			}
			else
			{
				return name;
			}
		}
	}
}
