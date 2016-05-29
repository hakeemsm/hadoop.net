using System;
using System.Security;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>Plugin to calculate resource information on the system.</summary>
	public abstract class ResourceCalculatorPlugin : Configured
	{
		/// <summary>Obtain the total size of the virtual memory present in the system.</summary>
		/// <returns>virtual memory size in bytes.</returns>
		public abstract long GetVirtualMemorySize();

		/// <summary>Obtain the total size of the physical memory present in the system.</summary>
		/// <returns>physical memory size bytes.</returns>
		public abstract long GetPhysicalMemorySize();

		/// <summary>
		/// Obtain the total size of the available virtual memory present
		/// in the system.
		/// </summary>
		/// <returns>available virtual memory size in bytes.</returns>
		public abstract long GetAvailableVirtualMemorySize();

		/// <summary>
		/// Obtain the total size of the available physical memory present
		/// in the system.
		/// </summary>
		/// <returns>available physical memory size bytes.</returns>
		public abstract long GetAvailablePhysicalMemorySize();

		/// <summary>Obtain the total number of processors present on the system.</summary>
		/// <returns>number of processors</returns>
		public abstract int GetNumProcessors();

		/// <summary>Obtain the CPU frequency of on the system.</summary>
		/// <returns>CPU frequency in kHz</returns>
		public abstract long GetCpuFrequency();

		/// <summary>Obtain the cumulative CPU time since the system is on.</summary>
		/// <returns>cumulative CPU time in milliseconds</returns>
		public abstract long GetCumulativeCpuTime();

		/// <summary>Obtain the CPU usage % of the machine.</summary>
		/// <remarks>Obtain the CPU usage % of the machine. Return -1 if it is unavailable</remarks>
		/// <returns>CPU usage in %</returns>
		public abstract float GetCpuUsage();

		/// <summary>Create the ResourceCalculatorPlugin from the class name and configure it.
		/// 	</summary>
		/// <remarks>
		/// Create the ResourceCalculatorPlugin from the class name and configure it. If
		/// class name is null, this method will try and return a memory calculator
		/// plugin available for this system.
		/// </remarks>
		/// <param name="clazz">ResourceCalculator plugin class-name</param>
		/// <param name="conf">configure the plugin with this.</param>
		/// <returns>
		/// ResourceCalculatorPlugin or null if ResourceCalculatorPlugin is not
		/// available for current system
		/// </returns>
		public static ResourceCalculatorPlugin GetResourceCalculatorPlugin(Type clazz, Configuration
			 conf)
		{
			if (clazz != null)
			{
				return ReflectionUtils.NewInstance(clazz, conf);
			}
			// No class given, try a os specific class
			try
			{
				if (Shell.Linux)
				{
					return new LinuxResourceCalculatorPlugin();
				}
				if (Shell.Windows)
				{
					return new WindowsResourceCalculatorPlugin();
				}
			}
			catch (SecurityException)
			{
				// Failed to get Operating System name.
				return null;
			}
			// Not supported on this system.
			return null;
		}
	}
}
