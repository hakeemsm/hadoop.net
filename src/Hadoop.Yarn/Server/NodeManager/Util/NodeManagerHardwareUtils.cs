using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class NodeManagerHardwareUtils
	{
		/// <summary>Returns the fraction of CPU cores that should be used for YARN containers.
		/// 	</summary>
		/// <remarks>
		/// Returns the fraction of CPU cores that should be used for YARN containers.
		/// The number is derived based on various configuration params such as
		/// YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
		/// </remarks>
		/// <param name="conf">- Configuration object</param>
		/// <returns>Fraction of CPU cores to be used for YARN containers</returns>
		public static float GetContainersCores(Configuration conf)
		{
			ResourceCalculatorPlugin plugin = ResourceCalculatorPlugin.GetResourceCalculatorPlugin
				(null, conf);
			return NodeManagerHardwareUtils.GetContainersCores(plugin, conf);
		}

		/// <summary>Returns the fraction of CPU cores that should be used for YARN containers.
		/// 	</summary>
		/// <remarks>
		/// Returns the fraction of CPU cores that should be used for YARN containers.
		/// The number is derived based on various configuration params such as
		/// YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
		/// </remarks>
		/// <param name="plugin">- ResourceCalculatorPlugin object to determine hardware specs
		/// 	</param>
		/// <param name="conf">- Configuration object</param>
		/// <returns>Fraction of CPU cores to be used for YARN containers</returns>
		public static float GetContainersCores(ResourceCalculatorPlugin plugin, Configuration
			 conf)
		{
			int numProcessors = plugin.GetNumProcessors();
			int nodeCpuPercentage = GetNodeCpuPercentage(conf);
			return (nodeCpuPercentage * numProcessors) / 100.0f;
		}

		/// <summary>Gets the percentage of physical CPU that is configured for YARN containers.
		/// 	</summary>
		/// <remarks>
		/// Gets the percentage of physical CPU that is configured for YARN containers.
		/// This is percent
		/// <literal>&gt;</literal>
		/// 0 and
		/// <literal>&lt;=</literal>
		/// 100 based on
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmResourcePercentagePhysicalCpuLimit
		/// 	"/>
		/// </remarks>
		/// <param name="conf">Configuration object</param>
		/// <returns>
		/// percent
		/// <literal>&gt;</literal>
		/// 0 and
		/// <literal>&lt;=</literal>
		/// 100
		/// </returns>
		public static int GetNodeCpuPercentage(Configuration conf)
		{
			int nodeCpuPercentage = Math.Min(conf.GetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit
				, YarnConfiguration.DefaultNmResourcePercentagePhysicalCpuLimit), 100);
			nodeCpuPercentage = Math.Max(0, nodeCpuPercentage);
			if (nodeCpuPercentage == 0)
			{
				string message = "Illegal value for " + YarnConfiguration.NmResourcePercentagePhysicalCpuLimit
					 + ". Value cannot be less than or equal to 0.";
				throw new ArgumentException(message);
			}
			return nodeCpuPercentage;
		}
	}
}
