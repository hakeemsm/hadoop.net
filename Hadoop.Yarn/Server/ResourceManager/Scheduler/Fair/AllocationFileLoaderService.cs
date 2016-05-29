using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Javax.Xml.Parsers;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.W3c.Dom;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class AllocationFileLoaderService : AbstractService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationFileLoaderService
			).FullName);

		/// <summary>Time to wait between checks of the allocation file</summary>
		public const long AllocReloadIntervalMs = 10 * 1000;

		/// <summary>
		/// Time to wait after the allocation has been modified before reloading it
		/// (this is done to prevent loading a file that hasn't been fully written).
		/// </summary>
		public const long AllocReloadWaitMs = 5 * 1000;

		public const long ThreadJoinTimeoutMs = 1000;

		private readonly Clock clock;

		private long lastSuccessfulReload;

		private bool lastReloadAttemptFailed = false;

		private FilePath allocFile;

		private AllocationFileLoaderService.Listener reloadListener;

		[VisibleForTesting]
		internal long reloadIntervalMs = AllocReloadIntervalMs;

		private Sharpen.Thread reloadThread;

		private volatile bool running = true;

		public AllocationFileLoaderService()
			: this(new SystemClock())
		{
		}

		public AllocationFileLoaderService(Clock clock)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationFileLoaderService
				).FullName)
		{
			// Last time we successfully reloaded queues
			// Path to XML file containing allocations. 
			this.clock = clock;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.allocFile = GetAllocationFile(conf);
			if (allocFile != null)
			{
				reloadThread = new _Thread_104(this);
				reloadThread.SetName("AllocationFileReloader");
				reloadThread.SetDaemon(true);
			}
			base.ServiceInit(conf);
		}

		private sealed class _Thread_104 : Sharpen.Thread
		{
			public _Thread_104(AllocationFileLoaderService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				while (this._enclosing.running)
				{
					long time = this._enclosing.clock.GetTime();
					long lastModified = this._enclosing.allocFile.LastModified();
					if (lastModified > this._enclosing.lastSuccessfulReload && time > lastModified + 
						Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationFileLoaderService
						.AllocReloadWaitMs)
					{
						try
						{
							this._enclosing.ReloadAllocations();
						}
						catch (Exception ex)
						{
							if (!this._enclosing.lastReloadAttemptFailed)
							{
								Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationFileLoaderService
									.Log.Error("Failed to reload fair scheduler config file - " + "will use existing allocations."
									, ex);
							}
							this._enclosing.lastReloadAttemptFailed = true;
						}
					}
					else
					{
						if (lastModified == 0l)
						{
							if (!this._enclosing.lastReloadAttemptFailed)
							{
								Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationFileLoaderService
									.Log.Warn("Failed to reload fair scheduler config file because" + " last modified returned 0. File exists: "
									 + this._enclosing.allocFile.Exists());
							}
							this._enclosing.lastReloadAttemptFailed = true;
						}
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.reloadIntervalMs);
					}
					catch (Exception)
					{
						Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationFileLoaderService
							.Log.Info("Interrupted while waiting to reload alloc configuration");
					}
				}
			}

			private readonly AllocationFileLoaderService _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (reloadThread != null)
			{
				reloadThread.Start();
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			running = false;
			if (reloadThread != null)
			{
				reloadThread.Interrupt();
				try
				{
					reloadThread.Join(ThreadJoinTimeoutMs);
				}
				catch (Exception)
				{
					Log.Warn("reloadThread fails to join.");
				}
			}
			base.ServiceStop();
		}

		/// <summary>Path to XML file containing allocations.</summary>
		/// <remarks>
		/// Path to XML file containing allocations. If the
		/// path is relative, it is searched for in the
		/// classpath, but loaded like a regular File.
		/// </remarks>
		public virtual FilePath GetAllocationFile(Configuration conf)
		{
			string allocFilePath = conf.Get(FairSchedulerConfiguration.AllocationFile, FairSchedulerConfiguration
				.DefaultAllocationFile);
			FilePath allocFile = new FilePath(allocFilePath);
			if (!allocFile.IsAbsolute())
			{
				Uri url = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource(allocFilePath
					);
				if (url == null)
				{
					Log.Warn(allocFilePath + " not found on the classpath.");
					allocFile = null;
				}
				else
				{
					if (!Sharpen.Runtime.EqualsIgnoreCase(url.Scheme, "file"))
					{
						throw new RuntimeException("Allocation file " + url + " found on the classpath is not on the local filesystem."
							);
					}
					else
					{
						allocFile = new FilePath(url.AbsolutePath);
					}
				}
			}
			return allocFile;
		}

		public virtual void SetReloadListener(AllocationFileLoaderService.Listener reloadListener
			)
		{
			lock (this)
			{
				this.reloadListener = reloadListener;
			}
		}

		/// <summary>Updates the allocation list from the allocation config file.</summary>
		/// <remarks>
		/// Updates the allocation list from the allocation config file. This file is
		/// expected to be in the XML format specified in the design doc.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if the config file cannot be read.</exception>
		/// <exception cref="AllocationConfigurationException">if allocations are invalid.</exception>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException">if XML parser is misconfigured.
		/// 	</exception>
		/// <exception cref="Org.Xml.Sax.SAXException">if config file is malformed.</exception>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public virtual void ReloadAllocations()
		{
			lock (this)
			{
				if (allocFile == null)
				{
					return;
				}
				Log.Info("Loading allocation file " + allocFile);
				// Create some temporary hashmaps to hold the new allocs, and we only save
				// them in our fields if we have parsed the entire allocs file successfully.
				IDictionary<string, Resource> minQueueResources = new Dictionary<string, Resource
					>();
				IDictionary<string, Resource> maxQueueResources = new Dictionary<string, Resource
					>();
				IDictionary<string, int> queueMaxApps = new Dictionary<string, int>();
				IDictionary<string, int> userMaxApps = new Dictionary<string, int>();
				IDictionary<string, float> queueMaxAMShares = new Dictionary<string, float>();
				IDictionary<string, ResourceWeights> queueWeights = new Dictionary<string, ResourceWeights
					>();
				IDictionary<string, SchedulingPolicy> queuePolicies = new Dictionary<string, SchedulingPolicy
					>();
				IDictionary<string, long> minSharePreemptionTimeouts = new Dictionary<string, long
					>();
				IDictionary<string, long> fairSharePreemptionTimeouts = new Dictionary<string, long
					>();
				IDictionary<string, float> fairSharePreemptionThresholds = new Dictionary<string, 
					float>();
				IDictionary<string, IDictionary<QueueACL, AccessControlList>> queueAcls = new Dictionary
					<string, IDictionary<QueueACL, AccessControlList>>();
				ICollection<string> reservableQueues = new HashSet<string>();
				int userMaxAppsDefault = int.MaxValue;
				int queueMaxAppsDefault = int.MaxValue;
				float queueMaxAMShareDefault = 0.5f;
				long defaultFairSharePreemptionTimeout = long.MaxValue;
				long defaultMinSharePreemptionTimeout = long.MaxValue;
				float defaultFairSharePreemptionThreshold = 0.5f;
				SchedulingPolicy defaultSchedPolicy = SchedulingPolicy.DefaultPolicy;
				// Reservation global configuration knobs
				string planner = null;
				string reservationAgent = null;
				string reservationAdmissionPolicy = null;
				QueuePlacementPolicy newPlacementPolicy = null;
				// Remember all queue names so we can display them on web UI, etc.
				// configuredQueues is segregated based on whether it is a leaf queue
				// or a parent queue. This information is used for creating queues
				// and also for making queue placement decisions(QueuePlacementRule.java).
				IDictionary<FSQueueType, ICollection<string>> configuredQueues = new Dictionary<FSQueueType
					, ICollection<string>>();
				foreach (FSQueueType queueType in FSQueueType.Values())
				{
					configuredQueues[queueType] = new HashSet<string>();
				}
				// Read and parse the allocations file.
				DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
				docBuilderFactory.SetIgnoringComments(true);
				DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
				Document doc = builder.Parse(allocFile);
				Element root = doc.GetDocumentElement();
				if (!"allocations".Equals(root.GetTagName()))
				{
					throw new AllocationConfigurationException("Bad fair scheduler config " + "file: top-level element not <allocations>"
						);
				}
				NodeList elements = root.GetChildNodes();
				IList<Element> queueElements = new AList<Element>();
				Element placementPolicyElement = null;
				for (int i = 0; i < elements.GetLength(); i++)
				{
					Node node = elements.Item(i);
					if (node is Element)
					{
						Element element = (Element)node;
						if ("queue".Equals(element.GetTagName()) || "pool".Equals(element.GetTagName()))
						{
							queueElements.AddItem(element);
						}
						else
						{
							if ("user".Equals(element.GetTagName()))
							{
								string userName = element.GetAttribute("name");
								NodeList fields = element.GetChildNodes();
								for (int j = 0; j < fields.GetLength(); j++)
								{
									Node fieldNode = fields.Item(j);
									if (!(fieldNode is Element))
									{
										continue;
									}
									Element field = (Element)fieldNode;
									if ("maxRunningApps".Equals(field.GetTagName()))
									{
										string text = ((Text)field.GetFirstChild()).GetData().Trim();
										int val = System.Convert.ToInt32(text);
										userMaxApps[userName] = val;
									}
								}
							}
							else
							{
								if ("userMaxAppsDefault".Equals(element.GetTagName()))
								{
									string text = ((Text)element.GetFirstChild()).GetData().Trim();
									int val = System.Convert.ToInt32(text);
									userMaxAppsDefault = val;
								}
								else
								{
									if ("defaultFairSharePreemptionTimeout".Equals(element.GetTagName()))
									{
										string text = ((Text)element.GetFirstChild()).GetData().Trim();
										long val = long.Parse(text) * 1000L;
										defaultFairSharePreemptionTimeout = val;
									}
									else
									{
										if ("fairSharePreemptionTimeout".Equals(element.GetTagName()))
										{
											if (defaultFairSharePreemptionTimeout == long.MaxValue)
											{
												string text = ((Text)element.GetFirstChild()).GetData().Trim();
												long val = long.Parse(text) * 1000L;
												defaultFairSharePreemptionTimeout = val;
											}
										}
										else
										{
											if ("defaultMinSharePreemptionTimeout".Equals(element.GetTagName()))
											{
												string text = ((Text)element.GetFirstChild()).GetData().Trim();
												long val = long.Parse(text) * 1000L;
												defaultMinSharePreemptionTimeout = val;
											}
											else
											{
												if ("defaultFairSharePreemptionThreshold".Equals(element.GetTagName()))
												{
													string text = ((Text)element.GetFirstChild()).GetData().Trim();
													float val = float.ParseFloat(text);
													val = Math.Max(Math.Min(val, 1.0f), 0.0f);
													defaultFairSharePreemptionThreshold = val;
												}
												else
												{
													if ("queueMaxAppsDefault".Equals(element.GetTagName()))
													{
														string text = ((Text)element.GetFirstChild()).GetData().Trim();
														int val = System.Convert.ToInt32(text);
														queueMaxAppsDefault = val;
													}
													else
													{
														if ("queueMaxAMShareDefault".Equals(element.GetTagName()))
														{
															string text = ((Text)element.GetFirstChild()).GetData().Trim();
															float val = float.ParseFloat(text);
															val = Math.Min(val, 1.0f);
															queueMaxAMShareDefault = val;
														}
														else
														{
															if ("defaultQueueSchedulingPolicy".Equals(element.GetTagName()) || "defaultQueueSchedulingMode"
																.Equals(element.GetTagName()))
															{
																string text = ((Text)element.GetFirstChild()).GetData().Trim();
																defaultSchedPolicy = SchedulingPolicy.Parse(text);
															}
															else
															{
																if ("queuePlacementPolicy".Equals(element.GetTagName()))
																{
																	placementPolicyElement = element;
																}
																else
																{
																	if ("reservation-planner".Equals(element.GetTagName()))
																	{
																		string text = ((Text)element.GetFirstChild()).GetData().Trim();
																		planner = text;
																	}
																	else
																	{
																		if ("reservation-agent".Equals(element.GetTagName()))
																		{
																			string text = ((Text)element.GetFirstChild()).GetData().Trim();
																			reservationAgent = text;
																		}
																		else
																		{
																			if ("reservation-policy".Equals(element.GetTagName()))
																			{
																				string text = ((Text)element.GetFirstChild()).GetData().Trim();
																				reservationAdmissionPolicy = text;
																			}
																			else
																			{
																				Log.Warn("Bad element in allocations file: " + element.GetTagName());
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				// Load queue elements.  A root queue can either be included or omitted.  If
				// it's included, all other queues must be inside it.
				foreach (Element element_1 in queueElements)
				{
					string parent = "root";
					if (Sharpen.Runtime.EqualsIgnoreCase(element_1.GetAttribute("name"), "root"))
					{
						if (queueElements.Count > 1)
						{
							throw new AllocationConfigurationException("If configuring root queue," + " no other queues can be placed alongside it."
								);
						}
						parent = null;
					}
					LoadQueue(parent, element_1, minQueueResources, maxQueueResources, queueMaxApps, 
						userMaxApps, queueMaxAMShares, queueWeights, queuePolicies, minSharePreemptionTimeouts
						, fairSharePreemptionTimeouts, fairSharePreemptionThresholds, queueAcls, configuredQueues
						, reservableQueues);
				}
				// Load placement policy and pass it configured queues
				Configuration conf = GetConfig();
				if (placementPolicyElement != null)
				{
					newPlacementPolicy = QueuePlacementPolicy.FromXml(placementPolicyElement, configuredQueues
						, conf);
				}
				else
				{
					newPlacementPolicy = QueuePlacementPolicy.FromConfiguration(conf, configuredQueues
						);
				}
				// Set the min/fair share preemption timeout for the root queue
				if (!minSharePreemptionTimeouts.Contains(QueueManager.RootQueue))
				{
					minSharePreemptionTimeouts[QueueManager.RootQueue] = defaultMinSharePreemptionTimeout;
				}
				if (!fairSharePreemptionTimeouts.Contains(QueueManager.RootQueue))
				{
					fairSharePreemptionTimeouts[QueueManager.RootQueue] = defaultFairSharePreemptionTimeout;
				}
				// Set the fair share preemption threshold for the root queue
				if (!fairSharePreemptionThresholds.Contains(QueueManager.RootQueue))
				{
					fairSharePreemptionThresholds[QueueManager.RootQueue] = defaultFairSharePreemptionThreshold;
				}
				ReservationQueueConfiguration globalReservationQueueConfig = new ReservationQueueConfiguration
					();
				if (planner != null)
				{
					globalReservationQueueConfig.SetPlanner(planner);
				}
				if (reservationAdmissionPolicy != null)
				{
					globalReservationQueueConfig.SetReservationAdmissionPolicy(reservationAdmissionPolicy
						);
				}
				if (reservationAgent != null)
				{
					globalReservationQueueConfig.SetReservationAgent(reservationAgent);
				}
				AllocationConfiguration info = new AllocationConfiguration(minQueueResources, maxQueueResources
					, queueMaxApps, userMaxApps, queueWeights, queueMaxAMShares, userMaxAppsDefault, 
					queueMaxAppsDefault, queueMaxAMShareDefault, queuePolicies, defaultSchedPolicy, 
					minSharePreemptionTimeouts, fairSharePreemptionTimeouts, fairSharePreemptionThresholds
					, queueAcls, newPlacementPolicy, configuredQueues, globalReservationQueueConfig, 
					reservableQueues);
				lastSuccessfulReload = clock.GetTime();
				lastReloadAttemptFailed = false;
				reloadListener.OnReload(info);
			}
		}

		/// <summary>Loads a queue from a queue element in the configuration file</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		private void LoadQueue(string parentName, Element element, IDictionary<string, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			> minQueueResources, IDictionary<string, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			> maxQueueResources, IDictionary<string, int> queueMaxApps, IDictionary<string, 
			int> userMaxApps, IDictionary<string, float> queueMaxAMShares, IDictionary<string
			, ResourceWeights> queueWeights, IDictionary<string, SchedulingPolicy> queuePolicies
			, IDictionary<string, long> minSharePreemptionTimeouts, IDictionary<string, long
			> fairSharePreemptionTimeouts, IDictionary<string, float> fairSharePreemptionThresholds
			, IDictionary<string, IDictionary<QueueACL, AccessControlList>> queueAcls, IDictionary
			<FSQueueType, ICollection<string>> configuredQueues, ICollection<string> reservableQueues
			)
		{
			string queueName = element.GetAttribute("name");
			if (queueName.Contains("."))
			{
				throw new AllocationConfigurationException("Bad fair scheduler config " + "file: queue name ("
					 + queueName + ") shouldn't contain period.");
			}
			if (parentName != null)
			{
				queueName = parentName + "." + queueName;
			}
			IDictionary<QueueACL, AccessControlList> acls = new Dictionary<QueueACL, AccessControlList
				>();
			NodeList fields = element.GetChildNodes();
			bool isLeaf = true;
			for (int j = 0; j < fields.GetLength(); j++)
			{
				Node fieldNode = fields.Item(j);
				if (!(fieldNode is Element))
				{
					continue;
				}
				Element field = (Element)fieldNode;
				if ("minResources".Equals(field.GetTagName()))
				{
					string text = ((Text)field.GetFirstChild()).GetData().Trim();
					Org.Apache.Hadoop.Yarn.Api.Records.Resource val = FairSchedulerConfiguration.ParseResourceConfigValue
						(text);
					minQueueResources[queueName] = val;
				}
				else
				{
					if ("maxResources".Equals(field.GetTagName()))
					{
						string text = ((Text)field.GetFirstChild()).GetData().Trim();
						Org.Apache.Hadoop.Yarn.Api.Records.Resource val = FairSchedulerConfiguration.ParseResourceConfigValue
							(text);
						maxQueueResources[queueName] = val;
					}
					else
					{
						if ("maxRunningApps".Equals(field.GetTagName()))
						{
							string text = ((Text)field.GetFirstChild()).GetData().Trim();
							int val = System.Convert.ToInt32(text);
							queueMaxApps[queueName] = val;
						}
						else
						{
							if ("maxAMShare".Equals(field.GetTagName()))
							{
								string text = ((Text)field.GetFirstChild()).GetData().Trim();
								float val = float.ParseFloat(text);
								val = Math.Min(val, 1.0f);
								queueMaxAMShares[queueName] = val;
							}
							else
							{
								if ("weight".Equals(field.GetTagName()))
								{
									string text = ((Text)field.GetFirstChild()).GetData().Trim();
									double val = double.ParseDouble(text);
									queueWeights[queueName] = new ResourceWeights((float)val);
								}
								else
								{
									if ("minSharePreemptionTimeout".Equals(field.GetTagName()))
									{
										string text = ((Text)field.GetFirstChild()).GetData().Trim();
										long val = long.Parse(text) * 1000L;
										minSharePreemptionTimeouts[queueName] = val;
									}
									else
									{
										if ("fairSharePreemptionTimeout".Equals(field.GetTagName()))
										{
											string text = ((Text)field.GetFirstChild()).GetData().Trim();
											long val = long.Parse(text) * 1000L;
											fairSharePreemptionTimeouts[queueName] = val;
										}
										else
										{
											if ("fairSharePreemptionThreshold".Equals(field.GetTagName()))
											{
												string text = ((Text)field.GetFirstChild()).GetData().Trim();
												float val = float.ParseFloat(text);
												val = Math.Max(Math.Min(val, 1.0f), 0.0f);
												fairSharePreemptionThresholds[queueName] = val;
											}
											else
											{
												if ("schedulingPolicy".Equals(field.GetTagName()) || "schedulingMode".Equals(field
													.GetTagName()))
												{
													string text = ((Text)field.GetFirstChild()).GetData().Trim();
													SchedulingPolicy policy = SchedulingPolicy.Parse(text);
													queuePolicies[queueName] = policy;
												}
												else
												{
													if ("aclSubmitApps".Equals(field.GetTagName()))
													{
														string text = ((Text)field.GetFirstChild()).GetData();
														acls[QueueACL.SubmitApplications] = new AccessControlList(text);
													}
													else
													{
														if ("aclAdministerApps".Equals(field.GetTagName()))
														{
															string text = ((Text)field.GetFirstChild()).GetData();
															acls[QueueACL.AdministerQueue] = new AccessControlList(text);
														}
														else
														{
															if ("reservation".Equals(field.GetTagName()))
															{
																isLeaf = false;
																reservableQueues.AddItem(queueName);
																configuredQueues[FSQueueType.Parent].AddItem(queueName);
															}
															else
															{
																if ("queue".EndsWith(field.GetTagName()) || "pool".Equals(field.GetTagName()))
																{
																	LoadQueue(queueName, field, minQueueResources, maxQueueResources, queueMaxApps, userMaxApps
																		, queueMaxAMShares, queueWeights, queuePolicies, minSharePreemptionTimeouts, fairSharePreemptionTimeouts
																		, fairSharePreemptionThresholds, queueAcls, configuredQueues, reservableQueues);
																	isLeaf = false;
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (isLeaf)
			{
				// if a leaf in the alloc file is marked as type='parent'
				// then store it under 'parent'
				if ("parent".Equals(element.GetAttribute("type")))
				{
					configuredQueues[FSQueueType.Parent].AddItem(queueName);
				}
				else
				{
					configuredQueues[FSQueueType.Leaf].AddItem(queueName);
				}
			}
			else
			{
				if ("parent".Equals(element.GetAttribute("type")))
				{
					throw new AllocationConfigurationException("Both <reservation> and " + "type=\"parent\" found for queue "
						 + queueName + " which is " + "unsupported");
				}
				configuredQueues[FSQueueType.Parent].AddItem(queueName);
			}
			queueAcls[queueName] = acls;
			if (maxQueueResources.Contains(queueName) && minQueueResources.Contains(queueName
				) && !Resources.FitsIn(minQueueResources[queueName], maxQueueResources[queueName
				]))
			{
				Log.Warn(string.Format("Queue %s has max resources %s less than min resources %s"
					, queueName, maxQueueResources[queueName], minQueueResources[queueName]));
			}
		}

		public interface Listener
		{
			void OnReload(AllocationConfiguration info);
		}
	}
}
