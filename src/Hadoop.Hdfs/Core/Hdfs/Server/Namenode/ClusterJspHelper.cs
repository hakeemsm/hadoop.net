using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Org.Codehaus.Jackson.Type;
using Org.Znerd.Xmlenc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class generates the data that is needed to be displayed on cluster web
	/// console.
	/// </summary>
	internal class ClusterJspHelper
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ClusterJspHelper));

		public const string OverallStatus = "overall-status";

		public const string Dead = "Dead";

		private const string JmxQry = "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";

		/// <summary>JSP helper function that generates cluster health report.</summary>
		/// <remarks>
		/// JSP helper function that generates cluster health report.  When
		/// encountering exception while getting Namenode status, the exception will
		/// be listed on the page with corresponding stack trace.
		/// </remarks>
		internal virtual ClusterJspHelper.ClusterStatus GenerateClusterHealthReport()
		{
			ClusterJspHelper.ClusterStatus cs = new ClusterJspHelper.ClusterStatus();
			Configuration conf = new Configuration();
			IList<DFSUtil.ConfiguredNNAddress> nns = null;
			try
			{
				nns = DFSUtil.FlattenAddressMap(DFSUtil.GetNNServiceRpcAddresses(conf));
			}
			catch (Exception e)
			{
				// Could not build cluster status
				cs.SetError(e);
				return cs;
			}
			// Process each namenode and add it to ClusterStatus
			foreach (DFSUtil.ConfiguredNNAddress cnn in nns)
			{
				IPEndPoint isa = cnn.GetAddress();
				ClusterJspHelper.NamenodeMXBeanHelper nnHelper = null;
				try
				{
					nnHelper = new ClusterJspHelper.NamenodeMXBeanHelper(isa, conf);
					string mbeanProps = QueryMbean(nnHelper.httpAddress, conf);
					ClusterJspHelper.NamenodeStatus nn = nnHelper.GetNamenodeStatus(mbeanProps);
					if (cs.clusterid.IsEmpty() || cs.clusterid.Equals(string.Empty))
					{
						// Set clusterid only once
						cs.clusterid = nnHelper.GetClusterId(mbeanProps);
					}
					cs.AddNamenodeStatus(nn);
				}
				catch (Exception e)
				{
					// track exceptions encountered when connecting to namenodes
					cs.AddException(isa.GetHostName(), e);
					continue;
				}
			}
			return cs;
		}

		/// <summary>Helper function that generates the decommissioning report.</summary>
		/// <remarks>
		/// Helper function that generates the decommissioning report.  Connect to each
		/// Namenode over http via JmxJsonServlet to collect the data nodes status.
		/// </remarks>
		internal virtual ClusterJspHelper.DecommissionStatus GenerateDecommissioningReport
			()
		{
			string clusterid = string.Empty;
			Configuration conf = new Configuration();
			IList<DFSUtil.ConfiguredNNAddress> cnns = null;
			try
			{
				cnns = DFSUtil.FlattenAddressMap(DFSUtil.GetNNServiceRpcAddresses(conf));
			}
			catch (Exception e)
			{
				// catch any exception encountered other than connecting to namenodes
				ClusterJspHelper.DecommissionStatus dInfo = new ClusterJspHelper.DecommissionStatus
					(clusterid, e);
				return dInfo;
			}
			// Outer map key is datanode. Inner map key is namenode and the value is 
			// decom status of the datanode for the corresponding namenode
			IDictionary<string, IDictionary<string, string>> statusMap = new Dictionary<string
				, IDictionary<string, string>>();
			// Map of exceptions encountered when connecting to namenode
			// key is namenode and value is exception
			IDictionary<string, Exception> decommissionExceptions = new Dictionary<string, Exception
				>();
			IList<string> unreportedNamenode = new AList<string>();
			foreach (DFSUtil.ConfiguredNNAddress cnn in cnns)
			{
				IPEndPoint isa = cnn.GetAddress();
				ClusterJspHelper.NamenodeMXBeanHelper nnHelper = null;
				try
				{
					nnHelper = new ClusterJspHelper.NamenodeMXBeanHelper(isa, conf);
					string mbeanProps = QueryMbean(nnHelper.httpAddress, conf);
					if (clusterid.Equals(string.Empty))
					{
						clusterid = nnHelper.GetClusterId(mbeanProps);
					}
					nnHelper.GetDecomNodeInfoForReport(statusMap, mbeanProps);
				}
				catch (Exception e)
				{
					// catch exceptions encountered while connecting to namenodes
					string nnHost = isa.GetHostName();
					decommissionExceptions[nnHost] = e;
					unreportedNamenode.AddItem(nnHost);
					continue;
				}
			}
			UpdateUnknownStatus(statusMap, unreportedNamenode);
			GetDecommissionNodeClusterState(statusMap);
			return new ClusterJspHelper.DecommissionStatus(statusMap, clusterid, GetDatanodeHttpPort
				(conf), decommissionExceptions);
		}

		/// <summary>
		/// Based on the state of the datanode at each namenode, marks the overall
		/// state of the datanode across all the namenodes, to one of the following:
		/// <ol>
		/// <li>
		/// <see cref="DecommissionStates.Decommissioned"/>
		/// </li>
		/// <li>
		/// <see cref="DecommissionStates.DecommissionInprogress"/>
		/// </li>
		/// <li>
		/// <see cref="DecommissionStates.PartiallyDecommissioned"/>
		/// </li>
		/// <li>
		/// <see cref="DecommissionStates.Unknown"/>
		/// </li>
		/// </ol>
		/// </summary>
		/// <param name="statusMap">
		/// map whose key is datanode, value is an inner map with key being
		/// namenode, value being decommission state.
		/// </param>
		private void GetDecommissionNodeClusterState(IDictionary<string, IDictionary<string
			, string>> statusMap)
		{
			if (statusMap == null || statusMap.IsEmpty())
			{
				return;
			}
			// For each datanodes
			IEnumerator<KeyValuePair<string, IDictionary<string, string>>> it = statusMap.GetEnumerator
				();
			while (it.HasNext())
			{
				// Map entry for a datanode:
				// key is namenode, value is datanode status at the namenode
				KeyValuePair<string, IDictionary<string, string>> entry = it.Next();
				IDictionary<string, string> nnStatus = entry.Value;
				if (nnStatus == null || nnStatus.IsEmpty())
				{
					continue;
				}
				bool isUnknown = false;
				int unknown = 0;
				int decommissioned = 0;
				int decomInProg = 0;
				int inservice = 0;
				int dead = 0;
				ClusterJspHelper.DecommissionStates overallState = ClusterJspHelper.DecommissionStates
					.Unknown;
				// Process a datanode state from each namenode
				foreach (KeyValuePair<string, string> m in nnStatus)
				{
					string status = m.Value;
					if (status.Equals(ClusterJspHelper.DecommissionStates.Unknown.ToString()))
					{
						isUnknown = true;
						unknown++;
					}
					else
					{
						if (status.Equals(DatanodeInfo.AdminStates.DecommissionInprogress.ToString()))
						{
							decomInProg++;
						}
						else
						{
							if (status.Equals(DatanodeInfo.AdminStates.Decommissioned.ToString()))
							{
								decommissioned++;
							}
							else
							{
								if (status.Equals(DatanodeInfo.AdminStates.Normal.ToString()))
								{
									inservice++;
								}
								else
								{
									if (status.Equals(Dead))
									{
										// dead
										dead++;
									}
								}
							}
						}
					}
				}
				// Consolidate all the states from namenode in to overall state
				int nns = nnStatus.Keys.Count;
				if ((inservice + dead + unknown) == nns)
				{
					// Do not display this data node. Remove this entry from status map.  
					it.Remove();
				}
				else
				{
					if (isUnknown)
					{
						overallState = ClusterJspHelper.DecommissionStates.Unknown;
					}
					else
					{
						if (decommissioned == nns)
						{
							overallState = ClusterJspHelper.DecommissionStates.Decommissioned;
						}
						else
						{
							if ((decommissioned + decomInProg) == nns)
							{
								overallState = ClusterJspHelper.DecommissionStates.DecommissionInprogress;
							}
							else
							{
								if ((decommissioned + decomInProg < nns) && (decommissioned + decomInProg > 0))
								{
									overallState = ClusterJspHelper.DecommissionStates.PartiallyDecommissioned;
								}
								else
								{
									Log.Warn("Cluster console encounters a not handled situtation.");
								}
							}
						}
					}
				}
				// insert overall state
				nnStatus[OverallStatus] = overallState.ToString();
			}
		}

		/// <summary>update unknown status in datanode status map for every unreported namenode
		/// 	</summary>
		private void UpdateUnknownStatus(IDictionary<string, IDictionary<string, string>>
			 statusMap, IList<string> unreportedNn)
		{
			if (unreportedNn == null || unreportedNn.IsEmpty())
			{
				// no unreported namenodes
				return;
			}
			foreach (KeyValuePair<string, IDictionary<string, string>> entry in statusMap)
			{
				string dn = entry.Key;
				IDictionary<string, string> nnStatus = entry.Value;
				foreach (string nn in unreportedNn)
				{
					nnStatus[nn] = ClusterJspHelper.DecommissionStates.Unknown.ToString();
				}
				statusMap[dn] = nnStatus;
			}
		}

		/// <summary>Get datanode http port from configration</summary>
		private int GetDatanodeHttpPort(Configuration conf)
		{
			string address = conf.Get(DFSConfigKeys.DfsDatanodeHttpAddressKey, string.Empty);
			if (address.Equals(string.Empty))
			{
				return -1;
			}
			return System.Convert.ToInt32(address.Split(":")[1]);
		}

		/// <summary>
		/// Class for connecting to Namenode over http via JmxJsonServlet
		/// to get JMX attributes exposed by the MXBean.
		/// </summary>
		internal class NamenodeMXBeanHelper
		{
			private static readonly ObjectMapper mapper = new ObjectMapper();

			private readonly string host;

			private readonly URI httpAddress;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Management.MalformedObjectNameException"/>
			internal NamenodeMXBeanHelper(IPEndPoint addr, Configuration conf)
			{
				this.host = addr.GetHostName();
				this.httpAddress = DFSUtil.GetInfoServer(addr, conf, DFSUtil.GetHttpClientScheme(
					conf));
			}

			/// <summary>Get the map corresponding to the JSON string</summary>
			/// <exception cref="System.IO.IOException"/>
			private static IDictionary<string, IDictionary<string, object>> GetNodeMap(string
				 json)
			{
				TypeReference<IDictionary<string, IDictionary<string, object>>> type = new _TypeReference_288
					();
				return mapper.ReadValue(json, type);
			}

			private sealed class _TypeReference_288 : TypeReference<IDictionary<string, IDictionary
				<string, object>>>
			{
				public _TypeReference_288()
				{
				}
			}

			/// <summary>Get the number of live datanodes.</summary>
			/// <param name="json">JSON string that contains live node status.</param>
			/// <param name="nn">namenode status to return information in</param>
			/// <exception cref="System.IO.IOException"/>
			private static void GetLiveNodeCount(string json, ClusterJspHelper.NamenodeStatus
				 nn)
			{
				// Map of datanode host to (map of attribute name to value)
				IDictionary<string, IDictionary<string, object>> nodeMap = GetNodeMap(json);
				if (nodeMap == null || nodeMap.IsEmpty())
				{
					return;
				}
				nn.liveDatanodeCount = nodeMap.Count;
				foreach (KeyValuePair<string, IDictionary<string, object>> entry in nodeMap)
				{
					// Inner map of attribute name to value
					IDictionary<string, object> innerMap = entry.Value;
					if (innerMap != null)
					{
						if (innerMap["adminState"].Equals(DatanodeInfo.AdminStates.Decommissioned.ToString
							()))
						{
							nn.liveDecomCount++;
						}
					}
				}
			}

			/// <summary>Count the number of dead datanode.</summary>
			/// <param name="nn">namenode</param>
			/// <param name="json">JSON string</param>
			/// <exception cref="System.IO.IOException"/>
			private static void GetDeadNodeCount(string json, ClusterJspHelper.NamenodeStatus
				 nn)
			{
				IDictionary<string, IDictionary<string, object>> nodeMap = GetNodeMap(json);
				if (nodeMap == null || nodeMap.IsEmpty())
				{
					return;
				}
				nn.deadDatanodeCount = nodeMap.Count;
				foreach (KeyValuePair<string, IDictionary<string, object>> entry in nodeMap)
				{
					IDictionary<string, object> innerMap = entry.Value;
					if (innerMap != null && !innerMap.IsEmpty())
					{
						if (((bool)innerMap["decommissioned"]) == true)
						{
							nn.deadDecomCount++;
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetClusterId(string props)
			{
				return GetProperty(props, "ClusterId").GetTextValue();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Management.MalformedObjectNameException"/>
			/// <exception cref="System.FormatException"/>
			public virtual ClusterJspHelper.NamenodeStatus GetNamenodeStatus(string props)
			{
				ClusterJspHelper.NamenodeStatus nn = new ClusterJspHelper.NamenodeStatus();
				nn.host = host;
				nn.filesAndDirectories = GetProperty(props, "TotalFiles").GetLongValue();
				nn.capacity = GetProperty(props, "Total").GetLongValue();
				nn.free = GetProperty(props, "Free").GetLongValue();
				nn.bpUsed = GetProperty(props, "BlockPoolUsedSpace").GetLongValue();
				nn.nonDfsUsed = GetProperty(props, "NonDfsUsedSpace").GetLongValue();
				nn.blocksCount = GetProperty(props, "TotalBlocks").GetLongValue();
				nn.missingBlocksCount = GetProperty(props, "NumberOfMissingBlocks").GetLongValue(
					);
				nn.httpAddress = httpAddress.ToURL();
				GetLiveNodeCount(GetProperty(props, "LiveNodes").AsText(), nn);
				GetDeadNodeCount(GetProperty(props, "DeadNodes").AsText(), nn);
				nn.softwareVersion = GetProperty(props, "SoftwareVersion").GetTextValue();
				return nn;
			}

			/// <summary>Get the decommission node information.</summary>
			/// <param name="statusMap">data node status map</param>
			/// <param name="props">string</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Management.MalformedObjectNameException"/>
			private void GetDecomNodeInfoForReport(IDictionary<string, IDictionary<string, string
				>> statusMap, string props)
			{
				GetLiveNodeStatus(statusMap, host, GetProperty(props, "LiveNodes").AsText());
				GetDeadNodeStatus(statusMap, host, GetProperty(props, "DeadNodes").AsText());
				GetDecommissionNodeStatus(statusMap, host, GetProperty(props, "DecomNodes").AsText
					());
			}

			/// <summary>
			/// Store the live datanode status information into datanode status map and
			/// DecommissionNode.
			/// </summary>
			/// <param name="statusMap">
			/// Map of datanode status. Key is datanode, value
			/// is an inner map whose key is namenode, value is datanode status.
			/// reported by each namenode.
			/// </param>
			/// <param name="namenodeHost">host name of the namenode</param>
			/// <param name="json">JSON string contains datanode status</param>
			/// <exception cref="System.IO.IOException"/>
			private static void GetLiveNodeStatus(IDictionary<string, IDictionary<string, string
				>> statusMap, string namenodeHost, string json)
			{
				IDictionary<string, IDictionary<string, object>> nodeMap = GetNodeMap(json);
				if (nodeMap != null && !nodeMap.IsEmpty())
				{
					IList<string> liveDecommed = new AList<string>();
					foreach (KeyValuePair<string, IDictionary<string, object>> entry in nodeMap)
					{
						IDictionary<string, object> innerMap = entry.Value;
						string dn = entry.Key;
						if (innerMap != null)
						{
							if (innerMap["adminState"].Equals(DatanodeInfo.AdminStates.Decommissioned.ToString
								()))
							{
								liveDecommed.AddItem(dn);
							}
							// the inner map key is namenode, value is datanode status.
							IDictionary<string, string> nnStatus = statusMap[dn];
							if (nnStatus == null)
							{
								nnStatus = new Dictionary<string, string>();
							}
							nnStatus[namenodeHost] = (string)innerMap["adminState"];
							// map whose key is datanode, value is the inner map.
							statusMap[dn] = nnStatus;
						}
					}
				}
			}

			/// <summary>
			/// Store the dead datanode information into datanode status map and
			/// DecommissionNode.
			/// </summary>
			/// <param name="statusMap">
			/// map with key being datanode, value being an
			/// inner map (key:namenode, value:decommisionning state).
			/// </param>
			/// <param name="host">datanode hostname</param>
			/// <param name="json">String</param>
			/// <exception cref="System.IO.IOException"/>
			private static void GetDeadNodeStatus(IDictionary<string, IDictionary<string, string
				>> statusMap, string host, string json)
			{
				IDictionary<string, IDictionary<string, object>> nodeMap = GetNodeMap(json);
				if (nodeMap == null || nodeMap.IsEmpty())
				{
					return;
				}
				IList<string> deadDn = new AList<string>();
				IList<string> deadDecommed = new AList<string>();
				foreach (KeyValuePair<string, IDictionary<string, object>> entry in nodeMap)
				{
					deadDn.AddItem(entry.Key);
					IDictionary<string, object> deadNodeDetailMap = entry.Value;
					string dn = entry.Key;
					if (deadNodeDetailMap != null && !deadNodeDetailMap.IsEmpty())
					{
						// NN - status
						IDictionary<string, string> nnStatus = statusMap[dn];
						if (nnStatus == null)
						{
							nnStatus = new Dictionary<string, string>();
						}
						if (((bool)deadNodeDetailMap["decommissioned"]) == true)
						{
							deadDecommed.AddItem(dn);
							nnStatus[host] = DatanodeInfo.AdminStates.Decommissioned.ToString();
						}
						else
						{
							nnStatus[host] = Dead;
						}
						// dn-nn-status
						statusMap[dn] = nnStatus;
					}
				}
			}

			/// <summary>Get the decommisioning datanode information.</summary>
			/// <param name="dataNodeStatusMap">
			/// map with key being datanode, value being an
			/// inner map (key:namenode, value:decommisionning state).
			/// </param>
			/// <param name="host">datanode</param>
			/// <param name="json">String</param>
			/// <exception cref="System.IO.IOException"/>
			private static void GetDecommissionNodeStatus(IDictionary<string, IDictionary<string
				, string>> dataNodeStatusMap, string host, string json)
			{
				IDictionary<string, IDictionary<string, object>> nodeMap = GetNodeMap(json);
				if (nodeMap == null || nodeMap.IsEmpty())
				{
					return;
				}
				IList<string> decomming = new AList<string>();
				foreach (KeyValuePair<string, IDictionary<string, object>> entry in nodeMap)
				{
					string dn = entry.Key;
					decomming.AddItem(dn);
					// nn-status
					IDictionary<string, string> nnStatus = new Dictionary<string, string>();
					if (dataNodeStatusMap.Contains(dn))
					{
						nnStatus = dataNodeStatusMap[dn];
					}
					nnStatus[host] = DatanodeInfo.AdminStates.DecommissionInprogress.ToString();
					// dn-nn-status
					dataNodeStatusMap[dn] = nnStatus;
				}
			}
		}

		/// <summary>This class contains cluster statistics.</summary>
		internal class ClusterStatus
		{
			/// <summary>Exception indicates failure to get cluster status</summary>
			internal Exception error = null;

			/// <summary>Cluster status information</summary>
			internal string clusterid = string.Empty;

			internal long total_sum = 0;

			internal long free_sum = 0;

			internal long clusterDfsUsed = 0;

			internal long nonDfsUsed_sum = 0;

			internal long totalFilesAndDirectories = 0;

			/// <summary>List of namenodes in the cluster</summary>
			internal readonly IList<ClusterJspHelper.NamenodeStatus> nnList = new AList<ClusterJspHelper.NamenodeStatus
				>();

			/// <summary>Map of namenode host and exception encountered when getting status</summary>
			internal readonly IDictionary<string, Exception> nnExceptions = new Dictionary<string
				, Exception>();

			public virtual void SetError(Exception e)
			{
				error = e;
			}

			public virtual void AddNamenodeStatus(ClusterJspHelper.NamenodeStatus nn)
			{
				nnList.AddItem(nn);
				// Add namenode status to cluster status
				totalFilesAndDirectories += nn.filesAndDirectories;
				total_sum += nn.capacity;
				free_sum += nn.free;
				clusterDfsUsed += nn.bpUsed;
				nonDfsUsed_sum += nn.nonDfsUsed;
			}

			public virtual void AddException(string host, Exception e)
			{
				nnExceptions[host] = e;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ToXML(XMLOutputter doc)
			{
				if (error != null)
				{
					// general exception, only print exception message onto web page.
					CreateGeneralException(doc, clusterid, StringUtils.StringifyException(error));
					doc.GetWriter().Flush();
					return;
				}
				int size = nnList.Count;
				long total = 0L;
				long free = 0L;
				long nonDfsUsed = 0l;
				float dfsUsedPercent = 0.0f;
				float dfsRemainingPercent = 0.0f;
				if (size > 0)
				{
					total = total_sum / size;
					free = free_sum / size;
					nonDfsUsed = nonDfsUsed_sum / size;
					dfsUsedPercent = DFSUtil.GetPercentUsed(clusterDfsUsed, total);
					dfsRemainingPercent = DFSUtil.GetPercentRemaining(free, total);
				}
				doc.StartTag("cluster");
				doc.Attribute("clusterId", clusterid);
				doc.StartTag("storage");
				ToXmlItemBlock(doc, "Total Files And Directories", System.Convert.ToString(totalFilesAndDirectories
					));
				ToXmlItemBlock(doc, "Configured Capacity", StringUtils.ByteDesc(total));
				ToXmlItemBlock(doc, "DFS Used", StringUtils.ByteDesc(clusterDfsUsed));
				ToXmlItemBlock(doc, "Non DFS Used", StringUtils.ByteDesc(nonDfsUsed));
				ToXmlItemBlock(doc, "DFS Remaining", StringUtils.ByteDesc(free));
				// dfsUsedPercent
				ToXmlItemBlock(doc, "DFS Used%", DFSUtil.Percent2String(dfsUsedPercent));
				// dfsRemainingPercent
				ToXmlItemBlock(doc, "DFS Remaining%", DFSUtil.Percent2String(dfsRemainingPercent)
					);
				doc.EndTag();
				// storage
				doc.StartTag("namenodes");
				// number of namenodes
				ToXmlItemBlock(doc, "NamenodesCount", Sharpen.Extensions.ToString(size));
				foreach (ClusterJspHelper.NamenodeStatus nn in nnList)
				{
					doc.StartTag("node");
					ToXmlItemBlockWithLink(doc, nn.host, nn.httpAddress, "NameNode");
					ToXmlItemBlock(doc, "Blockpool Used", StringUtils.ByteDesc(nn.bpUsed));
					ToXmlItemBlock(doc, "Blockpool Used%", DFSUtil.Percent2String(DFSUtil.GetPercentUsed
						(nn.bpUsed, total)));
					ToXmlItemBlock(doc, "Files And Directories", System.Convert.ToString(nn.filesAndDirectories
						));
					ToXmlItemBlock(doc, "Blocks", System.Convert.ToString(nn.blocksCount));
					ToXmlItemBlock(doc, "Missing Blocks", System.Convert.ToString(nn.missingBlocksCount
						));
					ToXmlItemBlockWithLink(doc, nn.liveDatanodeCount + " (" + nn.liveDecomCount + ")"
						, new Uri(nn.httpAddress, "/dfsnodelist.jsp?whatNodes=LIVE"), "Live Datanode (Decommissioned)"
						);
					ToXmlItemBlockWithLink(doc, nn.deadDatanodeCount + " (" + nn.deadDecomCount + ")"
						, new Uri(nn.httpAddress, "/dfsnodelist.jsp?whatNodes=DEAD"), "Dead Datanode (Decommissioned)"
						);
					ToXmlItemBlock(doc, "Software Version", nn.softwareVersion);
					doc.EndTag();
				}
				// node
				doc.EndTag();
				// namenodes
				CreateNamenodeExceptionMsg(doc, nnExceptions);
				doc.EndTag();
				// cluster
				doc.GetWriter().Flush();
			}
		}

		/// <summary>
		/// This class stores namenode statistics to be used to generate cluster
		/// web console report.
		/// </summary>
		internal class NamenodeStatus
		{
			internal string host = string.Empty;

			internal long capacity = 0L;

			internal long free = 0L;

			internal long bpUsed = 0L;

			internal long nonDfsUsed = 0L;

			internal long filesAndDirectories = 0L;

			internal long blocksCount = 0L;

			internal long missingBlocksCount = 0L;

			internal int liveDatanodeCount = 0;

			internal int liveDecomCount = 0;

			internal int deadDatanodeCount = 0;

			internal int deadDecomCount = 0;

			internal Uri httpAddress = null;

			internal string softwareVersion = string.Empty;
		}

		/// <summary>cluster-wide decommission state of a datanode</summary>
		[System.Serializable]
		public sealed class DecommissionStates
		{
			public static readonly ClusterJspHelper.DecommissionStates DecommissionInprogress
				 = new ClusterJspHelper.DecommissionStates("Decommission In Progress");

			public static readonly ClusterJspHelper.DecommissionStates Decommissioned = new ClusterJspHelper.DecommissionStates
				("Decommissioned");

			public static readonly ClusterJspHelper.DecommissionStates PartiallyDecommissioned
				 = new ClusterJspHelper.DecommissionStates("Partially Decommissioning");

			public static readonly ClusterJspHelper.DecommissionStates Unknown = new ClusterJspHelper.DecommissionStates
				("Unknown");

			internal readonly string value;

			internal DecommissionStates(string v)
			{
				/*
				* If datanode state is decommissioning at one or more namenodes and
				* decommissioned at the rest of the namenodes.
				*/
				/* If datanode state at all the namenodes is decommissioned */
				/*
				* If datanode state is not decommissioning at one or more namenodes and
				* decommissioned/decommissioning at the rest of the namenodes.
				*/
				/*
				* If datanode state is not known at a namenode, due to problems in getting
				* the datanode state from the namenode.
				*/
				this.value = v;
			}

			public override string ToString()
			{
				return ClusterJspHelper.DecommissionStates.value;
			}
		}

		/// <summary>
		/// This class consolidates the decommissioning datanodes information in the
		/// cluster and generates decommissioning reports in XML.
		/// </summary>
		internal class DecommissionStatus
		{
			/// <summary>Error when set indicates failure to get decomission status</summary>
			internal readonly Exception error;

			/// <summary>Map of dn host <-> (Map of NN host <-> decommissioning state)</summary>
			internal readonly IDictionary<string, IDictionary<string, string>> statusMap;

			internal readonly string clusterid;

			internal readonly int httpPort;

			internal int decommissioned = 0;

			internal int decommissioning = 0;

			internal int partial = 0;

			/// <summary>Map of namenode and exception encountered when getting decom status</summary>
			internal IDictionary<string, Exception> exceptions = new Dictionary<string, Exception
				>();

			private DecommissionStatus(IDictionary<string, IDictionary<string, string>> statusMap
				, string cid, int httpPort, IDictionary<string, Exception> exceptions)
				: this(statusMap, cid, httpPort, exceptions, null)
			{
			}

			public DecommissionStatus(string cid, Exception e)
				: this(null, cid, -1, null, e)
			{
			}

			private DecommissionStatus(IDictionary<string, IDictionary<string, string>> statusMap
				, string cid, int httpPort, IDictionary<string, Exception> exceptions, Exception
				 error)
			{
				// total number of decommissioned nodes
				// total number of decommissioning datanodes
				// total number of partially decommissioned nodes
				this.statusMap = statusMap;
				this.clusterid = cid;
				this.httpPort = httpPort;
				this.exceptions = exceptions;
				this.error = error;
			}

			/// <summary>Generate decommissioning datanode report in XML format</summary>
			/// <param name="doc">, xmloutputter</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ToXML(XMLOutputter doc)
			{
				if (error != null)
				{
					CreateGeneralException(doc, clusterid, StringUtils.StringifyException(error));
					doc.GetWriter().Flush();
					return;
				}
				if (statusMap == null || statusMap.IsEmpty())
				{
					// none of the namenodes has reported, print exceptions from each nn.
					doc.StartTag("cluster");
					CreateNamenodeExceptionMsg(doc, exceptions);
					doc.EndTag();
					doc.GetWriter().Flush();
					return;
				}
				doc.StartTag("cluster");
				doc.Attribute("clusterId", clusterid);
				doc.StartTag("decommissioningReport");
				CountDecommissionDatanodes();
				ToXmlItemBlock(doc, ClusterJspHelper.DecommissionStates.Decommissioned.ToString()
					, Sharpen.Extensions.ToString(decommissioned));
				ToXmlItemBlock(doc, ClusterJspHelper.DecommissionStates.DecommissionInprogress.ToString
					(), Sharpen.Extensions.ToString(decommissioning));
				ToXmlItemBlock(doc, ClusterJspHelper.DecommissionStates.PartiallyDecommissioned.ToString
					(), Sharpen.Extensions.ToString(partial));
				doc.EndTag();
				// decommissioningReport
				doc.StartTag("datanodes");
				ICollection<string> dnSet = statusMap.Keys;
				foreach (string dnhost in dnSet)
				{
					IDictionary<string, string> nnStatus = statusMap[dnhost];
					if (nnStatus == null || nnStatus.IsEmpty())
					{
						continue;
					}
					string overallStatus = nnStatus[OverallStatus];
					// check if datanode is in decommission states
					if (overallStatus != null && (overallStatus.Equals(DatanodeInfo.AdminStates.DecommissionInprogress
						.ToString()) || overallStatus.Equals(DatanodeInfo.AdminStates.Decommissioned.ToString
						()) || overallStatus.Equals(ClusterJspHelper.DecommissionStates.PartiallyDecommissioned
						.ToString()) || overallStatus.Equals(ClusterJspHelper.DecommissionStates.Unknown
						.ToString())))
					{
						doc.StartTag("node");
						// dn
						ToXmlItemBlockWithLink(doc, dnhost, new Uri("http", dnhost, httpPort, string.Empty
							), "DataNode");
						// overall status first
						ToXmlItemBlock(doc, OverallStatus, overallStatus);
						foreach (KeyValuePair<string, string> m in nnStatus)
						{
							string nn = m.Key;
							if (nn.Equals(OverallStatus))
							{
								continue;
							}
							// xml
							ToXmlItemBlock(doc, nn, nnStatus[nn]);
						}
						doc.EndTag();
					}
				}
				// node
				doc.EndTag();
				// datanodes
				CreateNamenodeExceptionMsg(doc, exceptions);
				doc.EndTag();
			}

			// cluster
			// toXML
			/// <summary>
			/// Count the total number of decommissioned/decommission_inprogress/
			/// partially decommissioned datanodes.
			/// </summary>
			private void CountDecommissionDatanodes()
			{
				foreach (string dn in statusMap.Keys)
				{
					IDictionary<string, string> nnStatus = statusMap[dn];
					string status = nnStatus[OverallStatus];
					if (status.Equals(ClusterJspHelper.DecommissionStates.Decommissioned.ToString()))
					{
						decommissioned++;
					}
					else
					{
						if (status.Equals(ClusterJspHelper.DecommissionStates.DecommissionInprogress.ToString
							()))
						{
							decommissioning++;
						}
						else
						{
							if (status.Equals(ClusterJspHelper.DecommissionStates.PartiallyDecommissioned.ToString
								()))
							{
								partial++;
							}
						}
					}
				}
			}
		}

		/// <summary>Generate a XML block as such, <item label=key value=value/></summary>
		/// <exception cref="System.IO.IOException"/>
		private static void ToXmlItemBlock(XMLOutputter doc, string key, string value)
		{
			doc.StartTag("item");
			doc.Attribute("label", key);
			doc.Attribute("value", value);
			doc.EndTag();
		}

		/// <summary>
		/// Generate a XML block as such, &lt;item label="Node" value="hostname"
		/// link="http://hostname:50070" /&gt;
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void ToXmlItemBlockWithLink(XMLOutputter doc, string value, Uri url
			, string label)
		{
			doc.StartTag("item");
			doc.Attribute("label", label);
			doc.Attribute("value", value);
			doc.Attribute("link", url.ToString());
			doc.EndTag();
		}

		// item
		/// <summary>
		/// create the XML for exceptions that we encountered when connecting to
		/// namenode.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void CreateNamenodeExceptionMsg(XMLOutputter doc, IDictionary<string
			, Exception> exceptionMsg)
		{
			if (exceptionMsg.Count > 0)
			{
				doc.StartTag("unreportedNamenodes");
				foreach (KeyValuePair<string, Exception> m in exceptionMsg)
				{
					doc.StartTag("node");
					doc.Attribute("name", m.Key);
					doc.Attribute("exception", StringUtils.StringifyException(m.Value));
					doc.EndTag();
				}
				// node
				doc.EndTag();
			}
		}

		// unreportedNamnodes
		/// <summary>create XML block from general exception.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void CreateGeneralException(XMLOutputter doc, string clusterid, string
			 eMsg)
		{
			doc.StartTag("cluster");
			doc.Attribute("clusterId", clusterid);
			doc.StartTag("message");
			doc.StartTag("item");
			doc.Attribute("msg", eMsg);
			doc.EndTag();
			// item
			doc.EndTag();
			// message
			doc.EndTag();
		}

		// cluster
		/// <summary>Read in the content from a URL</summary>
		/// <param name="url">URL To read</param>
		/// <returns>the text from the output</returns>
		/// <exception cref="System.IO.IOException">if something went wrong</exception>
		private static string ReadOutput(Uri url)
		{
			StringBuilder @out = new StringBuilder();
			URLConnection connection = url.OpenConnection();
			BufferedReader @in = new BufferedReader(new InputStreamReader(connection.GetInputStream
				(), Charsets.Utf8));
			string inputLine;
			while ((inputLine = @in.ReadLine()) != null)
			{
				@out.Append(inputLine);
			}
			@in.Close();
			return @out.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private static string QueryMbean(URI httpAddress, Configuration conf)
		{
			Uri url = new Uri(httpAddress.ToURL(), JmxQry);
			return ReadOutput(url);
		}

		/// <summary>
		/// In order to query a namenode mxbean, a http connection in the form of
		/// "http://hostname/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
		/// is sent to namenode.
		/// </summary>
		/// <remarks>
		/// In order to query a namenode mxbean, a http connection in the form of
		/// "http://hostname/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
		/// is sent to namenode.  JMX attributes are exposed via JmxJsonServelet on
		/// the namenode side.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static JsonNode GetProperty(string props, string propertyname)
		{
			if (props == null || props.Equals(string.Empty) || propertyname == null || propertyname
				.Equals(string.Empty))
			{
				return null;
			}
			ObjectMapper m = new ObjectMapper();
			JsonNode rootNode = m.ReadValue<JsonNode>(props);
			JsonNode jn = rootNode.Get("beans").Get(0).Get(propertyname);
			return jn;
		}
	}
}
