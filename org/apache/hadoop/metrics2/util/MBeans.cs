using Sharpen;

namespace org.apache.hadoop.metrics2.util
{
	/// <summary>
	/// This util class provides a method to register an MBean using
	/// our standard naming convention as described in the doc
	/// for {link
	/// <see cref="register(string, string, object)"/>
	/// </summary>
	public class MBeans
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.util.MBeans
			)));

		/// <summary>
		/// Register the MBean using our standard MBeanName format
		/// "hadoop:service=<serviceName>,name=<nameName>"
		/// Where the <serviceName> and <nameName> are the supplied parameters
		/// </summary>
		/// <param name="serviceName"/>
		/// <param name="nameName"/>
		/// <param name="theMbean">- the MBean to register</param>
		/// <returns>the named used to register the MBean</returns>
		public static javax.management.ObjectName register(string serviceName, string nameName
			, object theMbean)
		{
			javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer
				();
			javax.management.ObjectName name = getMBeanName(serviceName, nameName);
			try
			{
				mbs.registerMBean(theMbean, name);
				LOG.debug("Registered " + name);
				return name;
			}
			catch (javax.management.InstanceAlreadyExistsException iaee)
			{
				if (LOG.isTraceEnabled())
				{
					LOG.trace("Failed to register MBean \"" + name + "\"", iaee);
				}
				else
				{
					LOG.warn("Failed to register MBean \"" + name + "\": Instance already exists.");
				}
			}
			catch (System.Exception e)
			{
				LOG.warn("Failed to register MBean \"" + name + "\"", e);
			}
			return null;
		}

		public static void unregister(javax.management.ObjectName mbeanName)
		{
			LOG.debug("Unregistering " + mbeanName);
			javax.management.MBeanServer mbs = java.lang.management.ManagementFactory.getPlatformMBeanServer
				();
			if (mbeanName == null)
			{
				LOG.debug("Stacktrace: ", new System.Exception());
				return;
			}
			try
			{
				mbs.unregisterMBean(mbeanName);
			}
			catch (System.Exception e)
			{
				LOG.warn("Error unregistering " + mbeanName, e);
			}
			org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.removeMBeanName(mbeanName);
		}

		private static javax.management.ObjectName getMBeanName(string serviceName, string
			 nameName)
		{
			javax.management.ObjectName name = null;
			string nameStr = "Hadoop:service=" + serviceName + ",name=" + nameName;
			try
			{
				name = org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.newMBeanName(nameStr);
			}
			catch (System.Exception e)
			{
				LOG.warn("Error creating MBean object name: " + nameStr, e);
			}
			return name;
		}
	}
}
