using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	/// <summary>
	/// Unit test class to compare
	/// <see cref="YarnConfiguration"/>
	/// and
	/// yarn-default.xml for missing properties.  Currently only throws an error
	/// if the class is missing a property.
	/// <p></p>
	/// Refer to
	/// <see cref="Org.Apache.Hadoop.Conf.TestConfigurationFieldsBase"/>
	/// for how this class works.
	/// </summary>
	public class TestYarnConfigurationFields : TestConfigurationFieldsBase
	{
		public override void InitializeMemberVariables()
		{
			xmlFilename = new string("yarn-default.xml");
			configurationClasses = (Type[])new Type[] { typeof(YarnConfiguration) };
			// Allocate for usage
			configurationPropsToSkipCompare = new HashSet<string>();
			// Set error modes
			errorIfMissingConfigProps = true;
			errorIfMissingXmlProps = false;
			// Specific properties to skip
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultRmConfigurationProviderClass
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultClientFailoverProxyProvider
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultIpcRecordFactoryClass
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultIpcClientFactoryClass
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultIpcServerFactoryClass
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultIpcRpcImpl);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.DefaultRmScheduler);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.YarnSecurityServiceAuthorizationApplicationclientProtocol
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.YarnSecurityServiceAuthorizationApplicationmasterProtocol
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.YarnSecurityServiceAuthorizationContainerManagementProtocol
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.YarnSecurityServiceAuthorizationResourceLocalizer
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.YarnSecurityServiceAuthorizationResourcemanagerAdministrationProtocol
				);
			configurationPropsToSkipCompare.AddItem(YarnConfiguration.YarnSecurityServiceAuthorizationResourcetrackerProtocol
				);
			// Allocate for usage
			xmlPropsToSkipCompare = new HashSet<string>();
			xmlPrefixToSkipCompare = new HashSet<string>();
			// Should probably be moved from yarn-default.xml to mapred-default.xml
			xmlPropsToSkipCompare.AddItem("mapreduce.job.hdfs-servers");
			xmlPropsToSkipCompare.AddItem("mapreduce.job.jar");
			// Possibly obsolete, but unable to verify 100%
			xmlPropsToSkipCompare.AddItem("yarn.nodemanager.aux-services.mapreduce_shuffle.class"
				);
			xmlPropsToSkipCompare.AddItem("yarn.resourcemanager.container.liveness-monitor.interval-ms"
				);
			// Used in the XML file as a variable reference internal to the XML file
			xmlPropsToSkipCompare.AddItem("yarn.nodemanager.hostname");
			xmlPropsToSkipCompare.AddItem("yarn.timeline-service.hostname");
			// Currently defined in TimelineAuthenticationFilterInitializer
			xmlPrefixToSkipCompare.AddItem("yarn.timeline-service.http-authentication");
			// Currently defined in RegistryConstants
			xmlPrefixToSkipCompare.AddItem("hadoop.registry");
		}
	}
}
