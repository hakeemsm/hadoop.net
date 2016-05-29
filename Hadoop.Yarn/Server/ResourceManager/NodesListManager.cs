using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class NodesListManager : AbstractService, EventHandler<NodesListManagerEvent
		>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.NodesListManager
			));

		private HostsFileReader hostsReader;

		private Configuration conf;

		private ICollection<RMNode> unusableRMNodesConcurrentSet = Sharpen.Collections.NewSetFromMap
			(new ConcurrentHashMap<RMNode, bool>());

		private readonly RMContext rmContext;

		private string includesFile;

		private string excludesFile;

		public NodesListManager(RMContext rmContext)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.NodesListManager).FullName
				)
		{
			this.rmContext = rmContext;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.conf = conf;
			// Read the hosts/exclude files to restrict access to the RM
			try
			{
				this.includesFile = conf.Get(YarnConfiguration.RmNodesIncludeFilePath, YarnConfiguration
					.DefaultRmNodesIncludeFilePath);
				this.excludesFile = conf.Get(YarnConfiguration.RmNodesExcludeFilePath, YarnConfiguration
					.DefaultRmNodesExcludeFilePath);
				this.hostsReader = CreateHostsFileReader(this.includesFile, this.excludesFile);
				SetDecomissionedNMsMetrics();
				PrintConfiguredHosts();
			}
			catch (YarnException ex)
			{
				DisableHostsFileReader(ex);
			}
			catch (IOException ioe)
			{
				DisableHostsFileReader(ioe);
			}
			base.ServiceInit(conf);
		}

		private void PrintConfiguredHosts()
		{
			if (!Log.IsDebugEnabled())
			{
				return;
			}
			Log.Debug("hostsReader: in=" + conf.Get(YarnConfiguration.RmNodesIncludeFilePath, 
				YarnConfiguration.DefaultRmNodesIncludeFilePath) + " out=" + conf.Get(YarnConfiguration
				.RmNodesExcludeFilePath, YarnConfiguration.DefaultRmNodesExcludeFilePath));
			foreach (string include in hostsReader.GetHosts())
			{
				Log.Debug("include: " + include);
			}
			foreach (string exclude in hostsReader.GetExcludedHosts())
			{
				Log.Debug("exclude: " + exclude);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void RefreshNodes(Configuration yarnConf)
		{
			lock (hostsReader)
			{
				if (null == yarnConf)
				{
					yarnConf = new YarnConfiguration();
				}
				includesFile = yarnConf.Get(YarnConfiguration.RmNodesIncludeFilePath, YarnConfiguration
					.DefaultRmNodesIncludeFilePath);
				excludesFile = yarnConf.Get(YarnConfiguration.RmNodesExcludeFilePath, YarnConfiguration
					.DefaultRmNodesExcludeFilePath);
				hostsReader.UpdateFileNames(includesFile, excludesFile);
				hostsReader.Refresh(includesFile.IsEmpty() ? null : this.rmContext.GetConfigurationProvider
					().GetConfigurationInputStream(this.conf, includesFile), excludesFile.IsEmpty() ? 
					null : this.rmContext.GetConfigurationProvider().GetConfigurationInputStream(this
					.conf, excludesFile));
				PrintConfiguredHosts();
			}
			foreach (NodeId nodeId in rmContext.GetRMNodes().Keys)
			{
				if (!IsValidNode(nodeId.GetHost()))
				{
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeEvent(nodeId, RMNodeEventType
						.Decommission));
				}
			}
		}

		private void SetDecomissionedNMsMetrics()
		{
			ICollection<string> excludeList = hostsReader.GetExcludedHosts();
			ClusterMetrics.GetMetrics().SetDecommisionedNMs(excludeList.Count);
		}

		public virtual bool IsValidNode(string hostName)
		{
			lock (hostsReader)
			{
				ICollection<string> hostsList = hostsReader.GetHosts();
				ICollection<string> excludeList = hostsReader.GetExcludedHosts();
				string ip = NetUtils.NormalizeHostName(hostName);
				return (hostsList.IsEmpty() || hostsList.Contains(hostName) || hostsList.Contains
					(ip)) && !(excludeList.Contains(hostName) || excludeList.Contains(ip));
			}
		}

		/// <summary>Provides the currently unusable nodes.</summary>
		/// <remarks>Provides the currently unusable nodes. Copies it into provided collection.
		/// 	</remarks>
		/// <param name="unUsableNodes">Collection to which the unusable nodes are added</param>
		/// <returns>number of unusable nodes added</returns>
		public virtual int GetUnusableNodes(ICollection<RMNode> unUsableNodes)
		{
			Sharpen.Collections.AddAll(unUsableNodes, unusableRMNodesConcurrentSet);
			return unusableRMNodesConcurrentSet.Count;
		}

		public virtual void Handle(NodesListManagerEvent @event)
		{
			RMNode eventNode = @event.GetNode();
			switch (@event.GetType())
			{
				case NodesListManagerEventType.NodeUnusable:
				{
					Log.Debug(eventNode + " reported unusable");
					unusableRMNodesConcurrentSet.AddItem(eventNode);
					foreach (RMApp app in rmContext.GetRMApps().Values)
					{
						if (!app.IsAppFinalStateStored())
						{
							this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppNodeUpdateEvent(
								app.GetApplicationId(), eventNode, RMAppNodeUpdateEvent.RMAppNodeUpdateType.NodeUnusable
								));
						}
					}
					break;
				}

				case NodesListManagerEventType.NodeUsable:
				{
					if (unusableRMNodesConcurrentSet.Contains(eventNode))
					{
						Log.Debug(eventNode + " reported usable");
						unusableRMNodesConcurrentSet.Remove(eventNode);
					}
					foreach (RMApp app_1 in rmContext.GetRMApps().Values)
					{
						if (!app_1.IsAppFinalStateStored())
						{
							this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppNodeUpdateEvent(
								app_1.GetApplicationId(), eventNode, RMAppNodeUpdateEvent.RMAppNodeUpdateType.NodeUsable
								));
						}
					}
					break;
				}

				default:
				{
					Log.Error("Ignoring invalid eventtype " + @event.GetType());
					break;
				}
			}
		}

		private void DisableHostsFileReader(Exception ex)
		{
			Log.Warn("Failed to init hostsReader, disabling", ex);
			try
			{
				this.includesFile = conf.Get(YarnConfiguration.DefaultRmNodesIncludeFilePath);
				this.excludesFile = conf.Get(YarnConfiguration.DefaultRmNodesExcludeFilePath);
				this.hostsReader = CreateHostsFileReader(this.includesFile, this.excludesFile);
				SetDecomissionedNMsMetrics();
			}
			catch (IOException ioe2)
			{
				// Should *never* happen
				this.hostsReader = null;
				throw new YarnRuntimeException(ioe2);
			}
			catch (YarnException e)
			{
				// Should *never* happen
				this.hostsReader = null;
				throw new YarnRuntimeException(e);
			}
		}

		[VisibleForTesting]
		public virtual HostsFileReader GetHostsReader()
		{
			return this.hostsReader;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private HostsFileReader CreateHostsFileReader(string includesFile, string excludesFile
			)
		{
			HostsFileReader hostsReader = new HostsFileReader(includesFile, (includesFile == 
				null || includesFile.IsEmpty()) ? null : this.rmContext.GetConfigurationProvider
				().GetConfigurationInputStream(this.conf, includesFile), excludesFile, (excludesFile
				 == null || excludesFile.IsEmpty()) ? null : this.rmContext.GetConfigurationProvider
				().GetConfigurationInputStream(this.conf, excludesFile));
			return hostsReader;
		}
	}
}
