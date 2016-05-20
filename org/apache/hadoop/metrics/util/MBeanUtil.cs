using Sharpen;

namespace org.apache.hadoop.metrics.util
{
	/// <summary>
	/// This util class provides a method to register an MBean using
	/// our standard naming convention as described in the doc
	/// for {link
	/// <see cref="registerMBean(string, string, object)"/>
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
		public static javax.management.ObjectName registerMBean(string serviceName, string
			 nameName, object theMbean)
		{
			javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer
				();
			javax.management.ObjectName name = getMBeanName(serviceName, nameName);
			try
			{
				mbs.registerMBean(theMbean, name);
				return name;
			}
			catch (javax.management.InstanceAlreadyExistsException)
			{
			}
			catch (System.Exception e)
			{
				// Ignore if instance already exists 
				Sharpen.Runtime.printStackTrace(e);
			}
			return null;
		}

		public static void unregisterMBean(javax.management.ObjectName mbeanName)
		{
			javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer
				();
			if (mbeanName == null)
			{
				return;
			}
			try
			{
				mbs.unregisterMBean(mbeanName);
			}
			catch (javax.management.InstanceNotFoundException)
			{
			}
			catch (System.Exception e)
			{
				// ignore
				Sharpen.Runtime.printStackTrace(e);
			}
		}

		private static javax.management.ObjectName getMBeanName(string serviceName, string
			 nameName)
		{
			javax.management.ObjectName name = null;
			try
			{
				name = new javax.management.ObjectName("hadoop:" + "service=" + serviceName + ",name="
					 + nameName);
			}
			catch (javax.management.MalformedObjectNameException e)
			{
				Sharpen.Runtime.printStackTrace(e);
			}
			return name;
		}
	}
}
