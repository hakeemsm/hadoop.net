using System;
using Javax.Management;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Metrics.Util
{
	/// <summary>
	/// This util class provides a method to register an MBean using
	/// our standard naming convention as described in the doc
	/// for {link
	/// <see cref="RegisterMBean(string, string, object)"/>
	/// </summary>
	public class MBeanUtil
	{
		/// <summary>
		/// Register the MBean using our standard MBeanName format
		/// "hadoop:service=<serviceName>,name=<nameName>"
		/// Where the <serviceName> and <nameName> are the supplied parameters
		/// </summary>
		/// <param name="serviceName"/>
		/// <param name="nameName"/>
		/// <param name="theMbean">- the MBean to register</param>
		/// <returns>the named used to register the MBean</returns>
		public static ObjectName RegisterMBean(string serviceName, string nameName, object
			 theMbean)
		{
			MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
			ObjectName name = GetMBeanName(serviceName, nameName);
			try
			{
				mbs.RegisterMBean(theMbean, name);
				return name;
			}
			catch (InstanceAlreadyExistsException)
			{
			}
			catch (Exception e)
			{
				// Ignore if instance already exists 
				Sharpen.Runtime.PrintStackTrace(e);
			}
			return null;
		}

		public static void UnregisterMBean(ObjectName mbeanName)
		{
			MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
			if (mbeanName == null)
			{
				return;
			}
			try
			{
				mbs.UnregisterMBean(mbeanName);
			}
			catch (InstanceNotFoundException)
			{
			}
			catch (Exception e)
			{
				// ignore
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		private static ObjectName GetMBeanName(string serviceName, string nameName)
		{
			ObjectName name = null;
			try
			{
				name = new ObjectName("hadoop:" + "service=" + serviceName + ",name=" + nameName);
			}
			catch (MalformedObjectNameException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			return name;
		}
	}
}
