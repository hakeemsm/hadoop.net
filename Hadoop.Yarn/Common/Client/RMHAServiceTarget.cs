using System;
using System.Net;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class RMHAServiceTarget : HAServiceTarget
	{
		private readonly bool autoFailoverEnabled;

		private readonly IPEndPoint haAdminServiceAddress;

		/// <exception cref="System.IO.IOException"/>
		public RMHAServiceTarget(YarnConfiguration conf)
		{
			autoFailoverEnabled = HAUtil.IsAutomaticFailoverEnabled(conf);
			haAdminServiceAddress = conf.GetSocketAddr(YarnConfiguration.RmAdminAddress, YarnConfiguration
				.DefaultRmAdminAddress, YarnConfiguration.DefaultRmAdminPort);
		}

		public override IPEndPoint GetAddress()
		{
			return haAdminServiceAddress;
		}

		public override IPEndPoint GetZKFCAddress()
		{
			// TODO (YARN-1177): ZKFC implementation
			throw new NotSupportedException("RMHAServiceTarget doesn't have " + "a corresponding ZKFC address"
				);
		}

		public override NodeFencer GetFencer()
		{
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public override void CheckFencingConfigured()
		{
			throw new BadFencingConfigurationException("Fencer not configured");
		}

		public override bool IsAutoFailoverEnabled()
		{
			return autoFailoverEnabled;
		}
	}
}
