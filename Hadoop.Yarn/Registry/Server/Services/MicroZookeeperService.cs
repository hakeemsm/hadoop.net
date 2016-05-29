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
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Org.Apache.Curator.Ensemble.Fixed;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Service;
using Org.Apache.Zookeeper.Server;
using Org.Apache.Zookeeper.Server.Persistence;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Server.Services
{
	/// <summary>
	/// This is a small, localhost Zookeeper service instance that is contained
	/// in a YARN service...it's been derived from Apache Twill.
	/// </summary>
	/// <remarks>
	/// This is a small, localhost Zookeeper service instance that is contained
	/// in a YARN service...it's been derived from Apache Twill.
	/// <p>
	/// It implements
	/// <see cref="Org.Apache.Hadoop.Registry.Client.Impl.ZK.RegistryBindingSource"/>
	/// and provides binding information,
	/// <i>once started</i>. Until
	/// <see cref="Org.Apache.Hadoop.Service.AbstractService.Start()"/>
	/// is called, the hostname and
	/// port may be undefined. Accordingly, the service raises an exception in this
	/// condition.
	/// <p>
	/// If you wish to chain together a registry service with this one under
	/// the same
	/// <c>CompositeService</c>
	/// , this service must be added
	/// as a child first.
	/// <p>
	/// It also sets the configuration parameter
	/// <see cref="Org.Apache.Hadoop.Registry.Client.Api.RegistryConstants.KeyRegistryZkQuorum
	/// 	"/>
	/// to its connection string. Any code with access to the service configuration
	/// can view it.
	/// </remarks>
	public class MicroZookeeperService : AbstractService, RegistryBindingSource, RegistryConstants
		, ZookeeperConfigOptions, MicroZookeeperServiceKeys
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Server.Services.MicroZookeeperService
			));

		private FilePath instanceDir;

		private FilePath dataDir;

		private int tickTime;

		private int port;

		private string host;

		private bool secureServer;

		private ServerCnxnFactory factory;

		private BindingInformation binding;

		private FilePath confDir;

		private StringBuilder diagnostics = new StringBuilder();

		/// <summary>Create an instance</summary>
		/// <param name="name">service name</param>
		public MicroZookeeperService(string name)
			: base(name)
		{
		}

		/// <summary>Get the connection string.</summary>
		/// <returns>the string</returns>
		/// <exception cref="System.InvalidOperationException">if the connection is not yet valid
		/// 	</exception>
		public virtual string GetConnectionString()
		{
			Preconditions.CheckState(factory != null, "service not started");
			IPEndPoint addr = factory.GetLocalAddress();
			return string.Format("%s:%d", addr.GetHostName(), addr.Port);
		}

		/// <summary>Get the connection address</summary>
		/// <returns>the connection as an address</returns>
		/// <exception cref="System.InvalidOperationException">if the connection is not yet valid
		/// 	</exception>
		public virtual IPEndPoint GetConnectionAddress()
		{
			Preconditions.CheckState(factory != null, "service not started");
			return factory.GetLocalAddress();
		}

		/// <summary>Create an inet socket addr from the local host + port number</summary>
		/// <param name="port">port to use</param>
		/// <returns>a (hostname, port) pair</returns>
		/// <exception cref="Sharpen.UnknownHostException">if the server cannot resolve the host
		/// 	</exception>
		private IPEndPoint GetAddress(int port)
		{
			return new IPEndPoint(host, port < 0 ? 0 : port);
		}

		/// <summary>Initialize the service, including choosing a path for the data</summary>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			port = conf.GetInt(KeyZkservicePort, 0);
			tickTime = conf.GetInt(KeyZkserviceTickTime, ZooKeeperServer.DefaultTickTime);
			string instancedirname = conf.GetTrimmed(KeyZkserviceDir, string.Empty);
			host = conf.GetTrimmed(KeyZkserviceHost, DefaultZkserviceHost);
			if (instancedirname.IsEmpty())
			{
				FilePath testdir = new FilePath(Runtime.GetProperty("test.dir", "target"));
				instanceDir = new FilePath(testdir, "zookeeper" + GetName());
			}
			else
			{
				instanceDir = new FilePath(instancedirname);
				FileUtil.FullyDelete(instanceDir);
			}
			Log.Debug("Instance directory is {}", instanceDir);
			MkdirStrict(instanceDir);
			dataDir = new FilePath(instanceDir, "data");
			confDir = new FilePath(instanceDir, "conf");
			MkdirStrict(dataDir);
			MkdirStrict(confDir);
			base.ServiceInit(conf);
		}

		/// <summary>
		/// Create a directory, ignoring if the dir is already there,
		/// and failing if a file or something else was at the end of that
		/// path
		/// </summary>
		/// <param name="dir">dir to guarantee the existence of</param>
		/// <exception cref="System.IO.IOException">IO problems, or path exists but is not a dir
		/// 	</exception>
		private void MkdirStrict(FilePath dir)
		{
			if (!dir.Mkdirs())
			{
				if (!dir.IsDirectory())
				{
					throw new IOException("Failed to mkdir " + dir);
				}
			}
		}

		/// <summary>Append a formatted string to the diagnostics.</summary>
		/// <remarks>
		/// Append a formatted string to the diagnostics.
		/// <p>
		/// A newline is appended afterwards.
		/// </remarks>
		/// <param name="text">text including any format commands</param>
		/// <param name="args">arguments for the forma operation.</param>
		protected internal virtual void AddDiagnostics(string text, params object[] args)
		{
			diagnostics.Append(string.Format(text, args)).Append('\n');
		}

		/// <summary>Get the diagnostics info</summary>
		/// <returns>the diagnostics string built up</returns>
		public virtual string GetDiagnostics()
		{
			return diagnostics.ToString();
		}

		/// <summary>set up security.</summary>
		/// <remarks>
		/// set up security. this must be done prior to creating
		/// the ZK instance, as it sets up JAAS if that has not been done already.
		/// </remarks>
		/// <returns>true if the cluster has security enabled.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetupSecurity()
		{
			Configuration conf = GetConfig();
			string jaasContext = conf.GetTrimmed(KeyRegistryZkserviceJaasContext);
			secureServer = StringUtils.IsNotEmpty(jaasContext);
			if (secureServer)
			{
				RegistrySecurity.ValidateContext(jaasContext);
				RegistrySecurity.BindZKToServerJAASContext(jaasContext);
				// policy on failed auth
				Runtime.SetProperty(PropZkAllowFailedSaslClients, conf.Get(KeyZkserviceAllowFailedSaslClients
					, "true"));
				//needed so that you can use sasl: strings in the registry
				Runtime.SetProperty(RegistryInternalConstants.ZookeeperAuthProvider + ".1", RegistryInternalConstants
					.SaslauthenticationProvider);
				string serverContext = Runtime.GetProperty(PropZkServerSaslContext);
				AddDiagnostics("Server JAAS context s = %s", serverContext);
				return true;
			}
			else
			{
				return false;
			}
		}

		/// <summary>Startup: start ZK.</summary>
		/// <remarks>
		/// Startup: start ZK. It is only after this that
		/// the binding information is valid.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			SetupSecurity();
			ZooKeeperServer zkServer = new ZooKeeperServer();
			FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, dataDir);
			zkServer.SetTxnLogFactory(ftxn);
			zkServer.SetTickTime(tickTime);
			Log.Info("Starting Local Zookeeper service");
			factory = ServerCnxnFactory.CreateFactory();
			factory.Configure(GetAddress(port), -1);
			factory.Startup(zkServer);
			string connectString = GetConnectionString();
			Log.Info("In memory ZK started at {}\n", connectString);
			if (Log.IsDebugEnabled())
			{
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				zkServer.DumpConf(pw);
				pw.Flush();
				Log.Debug(sw.ToString());
			}
			binding = new BindingInformation();
			binding.ensembleProvider = new FixedEnsembleProvider(connectString);
			binding.description = GetName() + " reachable at \"" + connectString + "\"";
			AddDiagnostics(binding.description);
			// finally: set the binding information in the config
			GetConfig().Set(KeyRegistryZkQuorum, connectString);
		}

		/// <summary>
		/// When the service is stopped, it deletes the data directory
		/// and its contents
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (factory != null)
			{
				factory.Shutdown();
				factory = null;
			}
			if (dataDir != null)
			{
				FileUtil.FullyDelete(dataDir);
			}
		}

		public virtual BindingInformation SupplyBindingInformation()
		{
			Preconditions.CheckNotNull(binding, "Service is not started: binding information undefined"
				);
			return binding;
		}
	}
}
