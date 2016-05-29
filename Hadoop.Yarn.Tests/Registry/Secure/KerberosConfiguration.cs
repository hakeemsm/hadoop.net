/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using Javax.Security.Auth.Login;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Secure
{
	internal class KerberosConfiguration : Configuration
	{
		private string principal;

		private string keytab;

		private bool isInitiator;

		internal KerberosConfiguration(string principal, FilePath keytab, bool client)
		{
			this.principal = principal;
			this.keytab = keytab.GetAbsolutePath();
			this.isInitiator = client;
		}

		public static Configuration CreateClientConfig(string principal, FilePath keytab)
		{
			return new Org.Apache.Hadoop.Registry.Secure.KerberosConfiguration(principal, keytab
				, true);
		}

		public static Configuration CreateServerConfig(string principal, FilePath keytab)
		{
			return new Org.Apache.Hadoop.Registry.Secure.KerberosConfiguration(principal, keytab
				, false);
		}

		public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
		{
			IDictionary<string, string> options = new Dictionary<string, string>();
			options["keyTab"] = keytab;
			options["principal"] = principal;
			options["useKeyTab"] = "true";
			options["storeKey"] = "true";
			options["doNotPrompt"] = "true";
			options["useTicketCache"] = "true";
			options["renewTGT"] = "true";
			options["refreshKrb5Config"] = "true";
			options["isInitiator"] = bool.ToString(isInitiator);
			string ticketCache = Runtime.Getenv("KRB5CCNAME");
			if (ticketCache != null)
			{
				options["ticketCache"] = ticketCache;
			}
			options["debug"] = "true";
			return new AppConfigurationEntry[] { new AppConfigurationEntry(KerberosUtil.GetKrb5LoginModuleName
				(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
		}

		public override string ToString()
		{
			return "KerberosConfiguration with principal " + principal;
		}
	}
}
