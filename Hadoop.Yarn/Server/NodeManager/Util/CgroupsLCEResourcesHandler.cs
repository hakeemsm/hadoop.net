using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class CgroupsLCEResourcesHandler : LCEResourcesHandler
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util.CgroupsLCEResourcesHandler
			));

		private Configuration conf;

		private string cgroupPrefix;

		private bool cgroupMount;

		private string cgroupMountPath;

		private bool cpuWeightEnabled = true;

		private bool strictResourceUsageMode = false;

		private readonly string MtabFile = "/proc/mounts";

		private readonly string CgroupsFstype = "cgroup";

		private readonly string ControllerCpu = "cpu";

		private readonly string CpuPeriodUs = "cfs_period_us";

		private readonly string CpuQuotaUs = "cfs_quota_us";

		private readonly int CpuDefaultWeight = 1024;

		private readonly int MaxQuotaUs = 1000 * 1000;

		private readonly int MinPeriodUs = 1000;

		private readonly IDictionary<string, string> controllerPaths;

		private long deleteCgroupTimeout;

		private long deleteCgroupDelay;

		internal Clock clock;

		private float yarnProcessors;

		public CgroupsLCEResourcesHandler()
		{
			// set by kernel
			// Controller -> path
			// package private for testing purposes
			this.controllerPaths = new Dictionary<string, string>();
			clock = new SystemClock();
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void InitConfig()
		{
			this.cgroupPrefix = conf.Get(YarnConfiguration.NmLinuxContainerCgroupsHierarchy, 
				"/hadoop-yarn");
			this.cgroupMount = conf.GetBoolean(YarnConfiguration.NmLinuxContainerCgroupsMount
				, false);
			this.cgroupMountPath = conf.Get(YarnConfiguration.NmLinuxContainerCgroupsMountPath
				, null);
			this.deleteCgroupTimeout = conf.GetLong(YarnConfiguration.NmLinuxContainerCgroupsDeleteTimeout
				, YarnConfiguration.DefaultNmLinuxContainerCgroupsDeleteTimeout);
			this.deleteCgroupDelay = conf.GetLong(YarnConfiguration.NmLinuxContainerCgroupsDeleteDelay
				, YarnConfiguration.DefaultNmLinuxContainerCgroupsDeleteDelay);
			// remove extra /'s at end or start of cgroupPrefix
			if (cgroupPrefix[0] == '/')
			{
				cgroupPrefix = Sharpen.Runtime.Substring(cgroupPrefix, 1);
			}
			this.strictResourceUsageMode = conf.GetBoolean(YarnConfiguration.NmLinuxContainerCgroupsStrictResourceUsage
				, YarnConfiguration.DefaultNmLinuxContainerCgroupsStrictResourceUsage);
			int len = cgroupPrefix.Length;
			if (cgroupPrefix[len - 1] == '/')
			{
				cgroupPrefix = Sharpen.Runtime.Substring(cgroupPrefix, 0, len - 1);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Init(LinuxContainerExecutor lce)
		{
			this.Init(lce, ResourceCalculatorPlugin.GetResourceCalculatorPlugin(null, conf));
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void Init(LinuxContainerExecutor lce, ResourceCalculatorPlugin plugin
			)
		{
			InitConfig();
			// mount cgroups if requested
			if (cgroupMount && cgroupMountPath != null)
			{
				AList<string> cgroupKVs = new AList<string>();
				cgroupKVs.AddItem(ControllerCpu + "=" + cgroupMountPath + "/" + ControllerCpu);
				lce.MountCgroups(cgroupKVs, cgroupPrefix);
			}
			InitializeControllerPaths();
			// cap overall usage to the number of cores allocated to YARN
			yarnProcessors = NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
			int systemProcessors = plugin.GetNumProcessors();
			if (systemProcessors != (int)yarnProcessors)
			{
				Log.Info("YARN containers restricted to " + yarnProcessors + " cores");
				int[] limits = GetOverallLimits(yarnProcessors);
				UpdateCgroup(ControllerCpu, string.Empty, CpuPeriodUs, limits[0].ToString());
				UpdateCgroup(ControllerCpu, string.Empty, CpuQuotaUs, limits[1].ToString());
			}
			else
			{
				if (CpuLimitsExist())
				{
					Log.Info("Removing CPU constraints for YARN containers.");
					UpdateCgroup(ControllerCpu, string.Empty, CpuQuotaUs, (-1).ToString());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool CpuLimitsExist()
		{
			string path = PathForCgroup(ControllerCpu, string.Empty);
			FilePath quotaFile = new FilePath(path, ControllerCpu + "." + CpuQuotaUs);
			if (quotaFile.Exists())
			{
				string contents = FileUtils.ReadFileToString(quotaFile, "UTF-8");
				int quotaUS = System.Convert.ToInt32(contents.Trim());
				if (quotaUS != -1)
				{
					return true;
				}
			}
			return false;
		}

		[VisibleForTesting]
		internal virtual int[] GetOverallLimits(float yarnProcessors)
		{
			int[] ret = new int[2];
			if (yarnProcessors < 0.01f)
			{
				throw new ArgumentException("Number of processors can't be <= 0.");
			}
			int quotaUS = MaxQuotaUs;
			int periodUS = (int)(MaxQuotaUs / yarnProcessors);
			if (yarnProcessors < 1.0f)
			{
				periodUS = MaxQuotaUs;
				quotaUS = (int)(periodUS * yarnProcessors);
				if (quotaUS < MinPeriodUs)
				{
					Log.Warn("The quota calculated for the cgroup was too low. The minimum value is "
						 + MinPeriodUs + ", calculated value is " + quotaUS + ". Setting quota to minimum value."
						);
					quotaUS = MinPeriodUs;
				}
			}
			// cfs_period_us can't be less than 1000 microseconds
			// if the value of periodUS is less than 1000, we can't really use cgroups
			// to limit cpu
			if (periodUS < MinPeriodUs)
			{
				Log.Warn("The period calculated for the cgroup was too low. The minimum value is "
					 + MinPeriodUs + ", calculated value is " + periodUS + ". Using all available CPU."
					);
				periodUS = MaxQuotaUs;
				quotaUS = -1;
			}
			ret[0] = periodUS;
			ret[1] = quotaUS;
			return ret;
		}

		internal virtual bool IsCpuWeightEnabled()
		{
			return this.cpuWeightEnabled;
		}

		/*
		* Next four functions are for an individual cgroup.
		*/
		private string PathForCgroup(string controller, string groupName)
		{
			string controllerPath = controllerPaths[controller];
			return controllerPath + "/" + cgroupPrefix + "/" + groupName;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateCgroup(string controller, string groupName)
		{
			string path = PathForCgroup(controller, groupName);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("createCgroup: " + path);
			}
			if (!new FilePath(path).Mkdir())
			{
				throw new IOException("Failed to create cgroup at " + path);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateCgroup(string controller, string groupName, string param, string
			 value)
		{
			string path = PathForCgroup(controller, groupName);
			param = controller + "." + param;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("updateCgroup: " + path + ": " + param + "=" + value);
			}
			PrintWriter pw = null;
			try
			{
				FilePath file = new FilePath(path + "/" + param);
				TextWriter w = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
				pw = new PrintWriter(w);
				pw.Write(value);
			}
			catch (IOException e)
			{
				throw new IOException("Unable to set " + param + "=" + value + " for cgroup at: "
					 + path, e);
			}
			finally
			{
				if (pw != null)
				{
					bool hasError = pw.CheckError();
					pw.Close();
					if (hasError)
					{
						throw new IOException("Unable to set " + param + "=" + value + " for cgroup at: "
							 + path);
					}
					if (pw.CheckError())
					{
						throw new IOException("Error while closing cgroup file " + path);
					}
				}
			}
		}

		/*
		* Utility routine to print first line from cgroup tasks file
		*/
		private void LogLineFromTasksFile(FilePath cgf)
		{
			string str;
			if (Log.IsDebugEnabled())
			{
				try
				{
					using (BufferedReader inl = new BufferedReader(new InputStreamReader(new FileInputStream
						(cgf + "/tasks"), "UTF-8")))
					{
						if ((str = inl.ReadLine()) != null)
						{
							Log.Debug("First line in cgroup tasks file: " + cgf + " " + str);
						}
					}
				}
				catch (IOException e)
				{
					Log.Warn("Failed to read cgroup tasks file. ", e);
				}
			}
		}

		/// <summary>If tasks file is empty, delete the cgroup.</summary>
		/// <param name="file">object referring to the cgroup to be deleted</param>
		/// <returns>Boolean indicating whether cgroup was deleted</returns>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual bool CheckAndDeleteCgroup(FilePath cgf)
		{
			bool deleted = false;
			// FileInputStream in = null;
			try
			{
				using (FileInputStream @in = new FileInputStream(cgf + "/tasks"))
				{
					if (@in.Read() == -1)
					{
						/*
						* "tasks" file is empty, sleep a bit more and then try to delete the
						* cgroup. Some versions of linux will occasionally panic due to a race
						* condition in this area, hence the paranoia.
						*/
						Sharpen.Thread.Sleep(deleteCgroupDelay);
						deleted = cgf.Delete();
						if (!deleted)
						{
							Log.Warn("Failed attempt to delete cgroup: " + cgf);
						}
					}
					else
					{
						LogLineFromTasksFile(cgf);
					}
				}
			}
			catch (IOException e)
			{
				Log.Warn("Failed to read cgroup tasks file. ", e);
			}
			return deleted;
		}

		[VisibleForTesting]
		internal virtual bool DeleteCgroup(string cgroupPath)
		{
			bool deleted = false;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("deleteCgroup: " + cgroupPath);
			}
			long start = clock.GetTime();
			do
			{
				try
				{
					deleted = CheckAndDeleteCgroup(new FilePath(cgroupPath));
					if (!deleted)
					{
						Sharpen.Thread.Sleep(deleteCgroupDelay);
					}
				}
				catch (Exception)
				{
				}
			}
			while (!deleted && (clock.GetTime() - start) < deleteCgroupTimeout);
			// NOP
			if (!deleted)
			{
				Log.Warn("Unable to delete cgroup at: " + cgroupPath + ", tried to delete for " +
					 deleteCgroupTimeout + "ms");
			}
			return deleted;
		}

		/*
		* Next three functions operate on all the resources we are enforcing.
		*/
		/// <exception cref="System.IO.IOException"/>
		private void SetupLimits(ContainerId containerId, Resource containerResource)
		{
			string containerName = containerId.ToString();
			if (IsCpuWeightEnabled())
			{
				int containerVCores = containerResource.GetVirtualCores();
				CreateCgroup(ControllerCpu, containerName);
				int cpuShares = CpuDefaultWeight * containerVCores;
				UpdateCgroup(ControllerCpu, containerName, "shares", cpuShares.ToString());
				if (strictResourceUsageMode)
				{
					int nodeVCores = conf.GetInt(YarnConfiguration.NmVcores, YarnConfiguration.DefaultNmVcores
						);
					if (nodeVCores != containerVCores)
					{
						float containerCPU = (containerVCores * yarnProcessors) / (float)nodeVCores;
						int[] limits = GetOverallLimits(containerCPU);
						UpdateCgroup(ControllerCpu, containerName, CpuPeriodUs, limits[0].ToString());
						UpdateCgroup(ControllerCpu, containerName, CpuQuotaUs, limits[1].ToString());
					}
				}
			}
		}

		private void ClearLimits(ContainerId containerId)
		{
			if (IsCpuWeightEnabled())
			{
				DeleteCgroup(PathForCgroup(ControllerCpu, containerId.ToString()));
			}
		}

		/*
		* LCE Resources Handler interface
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void PreExecute(ContainerId containerId, Resource containerResource
			)
		{
			SetupLimits(containerId, containerResource);
		}

		public virtual void PostExecute(ContainerId containerId)
		{
			ClearLimits(containerId);
		}

		public virtual string GetResourcesOption(ContainerId containerId)
		{
			string containerName = containerId.ToString();
			StringBuilder sb = new StringBuilder("cgroups=");
			if (IsCpuWeightEnabled())
			{
				sb.Append(PathForCgroup(ControllerCpu, containerName) + "/tasks");
				sb.Append(",");
			}
			if (sb[sb.Length - 1] == ',')
			{
				Sharpen.Runtime.DeleteCharAt(sb, sb.Length - 1);
			}
			return sb.ToString();
		}

		private static readonly Sharpen.Pattern MtabFileFormat = Sharpen.Pattern.Compile(
			"^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");

		/* We are looking for entries of the form:
		* none /cgroup/path/mem cgroup rw,memory 0 0
		*
		* Use a simple pattern that splits on the five spaces, and
		* grabs the 2, 3, and 4th fields.
		*/
		/*
		* Returns a map: path -> mount options
		* for mounts with type "cgroup". Cgroup controllers will
		* appear in the list of options for a path.
		*/
		/// <exception cref="System.IO.IOException"/>
		private IDictionary<string, IList<string>> ParseMtab()
		{
			IDictionary<string, IList<string>> ret = new Dictionary<string, IList<string>>();
			BufferedReader @in = null;
			try
			{
				FileInputStream fis = new FileInputStream(new FilePath(GetMtabFileName()));
				@in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
				for (string str = @in.ReadLine(); str != null; str = @in.ReadLine())
				{
					Matcher m = MtabFileFormat.Matcher(str);
					bool mat = m.Find();
					if (mat)
					{
						string path = m.Group(1);
						string type = m.Group(2);
						string options = m.Group(3);
						if (type.Equals(CgroupsFstype))
						{
							IList<string> value = Arrays.AsList(options.Split(","));
							ret[path] = value;
						}
					}
				}
			}
			catch (IOException e)
			{
				throw new IOException("Error while reading " + GetMtabFileName(), e);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			return ret;
		}

		private string FindControllerInMtab(string controller, IDictionary<string, IList<
			string>> entries)
		{
			foreach (KeyValuePair<string, IList<string>> e in entries)
			{
				if (e.Value.Contains(controller))
				{
					return e.Key;
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitializeControllerPaths()
		{
			string controllerPath;
			IDictionary<string, IList<string>> parsedMtab = ParseMtab();
			// CPU
			controllerPath = FindControllerInMtab(ControllerCpu, parsedMtab);
			if (controllerPath != null)
			{
				FilePath f = new FilePath(controllerPath + "/" + this.cgroupPrefix);
				if (FileUtil.CanWrite(f))
				{
					controllerPaths[ControllerCpu] = controllerPath;
				}
				else
				{
					throw new IOException("Not able to enforce cpu weights; cannot write " + "to cgroup at: "
						 + controllerPath);
				}
			}
			else
			{
				throw new IOException("Not able to enforce cpu weights; cannot find " + "cgroup for cpu controller in "
					 + GetMtabFileName());
			}
		}

		[VisibleForTesting]
		internal virtual string GetMtabFileName()
		{
			return MtabFile;
		}
	}
}
