using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>Class to extend HAAdmin to do a little bit of HDFS-specific configuration.
	/// 	</summary>
	public class DFSHAAdmin : HAAdmin
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DFSHAAdmin));

		private string nameserviceId;

		protected internal virtual void SetErrOut(TextWriter errOut)
		{
			this.errOut = errOut;
		}

		protected internal virtual void SetOut(TextWriter @out)
		{
			this.@out = @out;
		}

		public override void SetConf(Configuration conf)
		{
			if (conf != null)
			{
				conf = AddSecurityConfiguration(conf);
			}
			base.SetConf(conf);
		}

		/// <summary>
		/// Add the requisite security principal settings to the given Configuration,
		/// returning a copy.
		/// </summary>
		/// <param name="conf">the original config</param>
		/// <returns>a copy with the security settings added</returns>
		public static Configuration AddSecurityConfiguration(Configuration conf)
		{
			// Make a copy so we don't mutate it. Also use an HdfsConfiguration to
			// force loading of hdfs-site.xml.
			conf = new HdfsConfiguration(conf);
			string nameNodePrincipal = conf.Get(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
				, string.Empty);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Using NN principal: " + nameNodePrincipal);
			}
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, nameNodePrincipal
				);
			return conf;
		}

		/// <summary>Try to map the given namenode ID to its service address.</summary>
		protected override HAServiceTarget ResolveTarget(string nnId)
		{
			HdfsConfiguration conf = (HdfsConfiguration)GetConf();
			return new NNHAServiceTarget(conf, nameserviceId, nnId);
		}

		protected override string GetUsageString()
		{
			return "Usage: haadmin";
		}

		/// <exception cref="System.Exception"/>
		protected override int RunCmd(string[] argv)
		{
			if (argv.Length < 1)
			{
				PrintUsage(errOut);
				return -1;
			}
			int i = 0;
			string cmd = argv[i++];
			if ("-ns".Equals(cmd))
			{
				if (i == argv.Length)
				{
					errOut.WriteLine("Missing nameservice ID");
					PrintUsage(errOut);
					return -1;
				}
				nameserviceId = argv[i++];
				if (i >= argv.Length)
				{
					errOut.WriteLine("Missing command");
					PrintUsage(errOut);
					return -1;
				}
				argv = Arrays.CopyOfRange(argv, i, argv.Length);
			}
			return base.RunCmd(argv);
		}

		/// <summary>returns the list of all namenode ids for the given configuration</summary>
		protected override ICollection<string> GetTargetIds(string namenodeToActivate)
		{
			return DFSUtil.GetNameNodeIds(GetConf(), (nameserviceId != null) ? nameserviceId : 
				DFSUtil.GetNamenodeNameServiceId(GetConf()));
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new DFSHAAdmin(), argv);
			System.Environment.Exit(res);
		}
	}
}
