using System;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>
	/// This util class provides a method to register an MBean using
	/// our standard naming convention as described in the doc
	/// for {link
	/// <see cref="Register(string, string, object)"/>
	/// </summary>
	public class MBeans
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MBeans));

		/// <summary>
		/// Register the MBean using our standard MBeanName format
		/// "hadoop:service=<serviceName>,name=<nameName>"
		/// Where the <serviceName> and <nameName> are the supplied parameters
		/// </summary>
		/// <param name="serviceName"/>
		/// <param name="nameName"/>
		/// <param name="theMbean">- the MBean to register</param>
		/// <returns>the named used to register the MBean</returns>
		public static ObjectName Register(string serviceName, string nameName, object theMbean
			)
		{
			MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
			ObjectName name = GetMBeanName(serviceName, nameName);
			try
			{
				mbs.RegisterMBean(theMbean, name);
				Log.Debug("Registered " + name);
				return name;
			}
			catch (InstanceAlreadyExistsException iaee)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Failed to register MBean \"" + name + "\"", iaee);
				}
				else
				{
					Log.Warn("Failed to register MBean \"" + name + "\": Instance already exists.");
				}
			}
			catch (Exception e)
			{
				Log.Warn("Failed to register MBean \"" + name + "\"", e);
			}
			return null;
		}

		public static void Unregister(ObjectName mbeanName)
		{
			Log.Debug("Unregistering " + mbeanName);
			MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
			if (mbeanName == null)
			{
				Log.Debug("Stacktrace: ", new Exception());
				return;
			}
			try
			{
				mbs.UnregisterMBean(mbeanName);
			}
			catch (Exception e)
			{
				Log.Warn("Error unregistering " + mbeanName, e);
			}
			DefaultMetricsSystem.RemoveMBeanName(mbeanName);
		}

		private static ObjectName GetMBeanName(string serviceName, string nameName)
		{
			ObjectName name = null;
			string nameStr = "Hadoop:service=" + serviceName + ",name=" + nameName;
			try
			{
				name = DefaultMetricsSystem.NewMBeanName(nameStr);
			}
			catch (Exception e)
			{
				Log.Warn("Error creating MBean object name: " + nameStr, e);
			}
			return name;
		}
	}
}
