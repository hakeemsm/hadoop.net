using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.W3c.Dom;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class QueuePlacementPolicy
	{
		private static readonly IDictionary<string, Type> ruleClasses;

		static QueuePlacementPolicy()
		{
			IDictionary<string, Type> map = new Dictionary<string, Type>();
			map["user"] = typeof(QueuePlacementRule.User);
			map["primaryGroup"] = typeof(QueuePlacementRule.PrimaryGroup);
			map["secondaryGroupExistingQueue"] = typeof(QueuePlacementRule.SecondaryGroupExistingQueue
				);
			map["specified"] = typeof(QueuePlacementRule.Specified);
			map["nestedUserQueue"] = typeof(QueuePlacementRule.NestedUserQueue);
			map["default"] = typeof(QueuePlacementRule.Default);
			map["reject"] = typeof(QueuePlacementRule.Reject);
			ruleClasses = Sharpen.Collections.UnmodifiableMap(map);
		}

		private readonly IList<QueuePlacementRule> rules;

		private readonly IDictionary<FSQueueType, ICollection<string>> configuredQueues;

		private readonly Groups groups;

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public QueuePlacementPolicy(IList<QueuePlacementRule> rules, IDictionary<FSQueueType
			, ICollection<string>> configuredQueues, Configuration conf)
		{
			for (int i = 0; i < rules.Count - 1; i++)
			{
				if (rules[i].IsTerminal())
				{
					throw new AllocationConfigurationException("Rules after rule " + i + " in queue placement policy can never be reached"
						);
				}
			}
			if (!rules[rules.Count - 1].IsTerminal())
			{
				throw new AllocationConfigurationException("Could get past last queue placement rule without assigning"
					);
			}
			this.rules = rules;
			this.configuredQueues = configuredQueues;
			groups = new Groups(conf);
		}

		/// <summary>Builds a QueuePlacementPolicy from an xml element.</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.QueuePlacementPolicy
			 FromXml(Element el, IDictionary<FSQueueType, ICollection<string>> configuredQueues
			, Configuration conf)
		{
			IList<QueuePlacementRule> rules = new AList<QueuePlacementRule>();
			NodeList elements = el.GetChildNodes();
			for (int i = 0; i < elements.GetLength(); i++)
			{
				Node node = elements.Item(i);
				if (node is Element)
				{
					QueuePlacementRule rule = CreateAndInitializeRule(node);
					rules.AddItem(rule);
				}
			}
			return new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.QueuePlacementPolicy
				(rules, configuredQueues, conf);
		}

		/// <summary>Create and initialize a rule given a xml node</summary>
		/// <param name="node"/>
		/// <returns>QueuePlacementPolicy</returns>
		/// <exception cref="AllocationConfigurationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public static QueuePlacementRule CreateAndInitializeRule(Node node)
		{
			Element element = (Element)node;
			string ruleName = element.GetAttribute("name");
			if (string.Empty.Equals(ruleName))
			{
				throw new AllocationConfigurationException("No name provided for a " + "rule element"
					);
			}
			Type clazz = ruleClasses[ruleName];
			if (clazz == null)
			{
				throw new AllocationConfigurationException("No rule class found for " + ruleName);
			}
			QueuePlacementRule rule = ReflectionUtils.NewInstance(clazz, null);
			rule.InitializeFromXml(element);
			return rule;
		}

		/// <summary>
		/// Build a simple queue placement policy from the allow-undeclared-pools and
		/// user-as-default-queue configuration options.
		/// </summary>
		public static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.QueuePlacementPolicy
			 FromConfiguration(Configuration conf, IDictionary<FSQueueType, ICollection<string
			>> configuredQueues)
		{
			bool create = conf.GetBoolean(FairSchedulerConfiguration.AllowUndeclaredPools, FairSchedulerConfiguration
				.DefaultAllowUndeclaredPools);
			bool userAsDefaultQueue = conf.GetBoolean(FairSchedulerConfiguration.UserAsDefaultQueue
				, FairSchedulerConfiguration.DefaultUserAsDefaultQueue);
			IList<QueuePlacementRule> rules = new AList<QueuePlacementRule>();
			rules.AddItem(new QueuePlacementRule.Specified().Initialize(create, null));
			if (userAsDefaultQueue)
			{
				rules.AddItem(new QueuePlacementRule.User().Initialize(create, null));
			}
			if (!userAsDefaultQueue || !create)
			{
				rules.AddItem(new QueuePlacementRule.Default().Initialize(true, null));
			}
			try
			{
				return new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.QueuePlacementPolicy
					(rules, configuredQueues, conf);
			}
			catch (AllocationConfigurationException ex)
			{
				throw new RuntimeException("Should never hit exception when loading" + "placement policy from conf"
					, ex);
			}
		}

		/// <summary>
		/// Applies this rule to an app with the given requested queue and user/group
		/// information.
		/// </summary>
		/// <param name="requestedQueue">The queue specified in the ApplicationSubmissionContext
		/// 	</param>
		/// <param name="user">The user submitting the app</param>
		/// <returns>
		/// The name of the queue to assign the app to.  Or null if the app should
		/// be rejected.
		/// </returns>
		/// <exception cref="System.IO.IOException">If an exception is encountered while getting the user's groups
		/// 	</exception>
		public virtual string AssignAppToQueue(string requestedQueue, string user)
		{
			foreach (QueuePlacementRule rule in rules)
			{
				string queue = rule.AssignAppToQueue(requestedQueue, user, groups, configuredQueues
					);
				if (queue == null || !queue.IsEmpty())
				{
					return queue;
				}
			}
			throw new InvalidOperationException("Should have applied a rule before " + "reaching here"
				);
		}

		public virtual IList<QueuePlacementRule> GetRules()
		{
			return rules;
		}
	}
}
