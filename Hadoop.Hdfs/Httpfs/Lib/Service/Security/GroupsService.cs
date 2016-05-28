using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Server;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Util;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Security
{
	public class GroupsService : BaseService, Groups
	{
		private const string Prefix = "groups";

		private Groups hGroups;

		public GroupsService()
			: base(Prefix)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		protected internal override void Init()
		{
			Configuration hConf = new Configuration(false);
			ConfigurationUtils.Copy(GetServiceConfig(), hConf);
			hGroups = new Groups(hConf);
		}

		public override Type GetInterface()
		{
			return typeof(Groups);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<string> GetGroups(string user)
		{
			return hGroups.GetGroups(user);
		}
	}
}
