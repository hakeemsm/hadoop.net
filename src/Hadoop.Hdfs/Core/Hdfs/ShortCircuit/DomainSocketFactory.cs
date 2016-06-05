using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	public class DomainSocketFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Shortcircuit.DomainSocketFactory
			));

		[System.Serializable]
		public sealed class PathState
		{
			public static readonly DomainSocketFactory.PathState Unusable = new DomainSocketFactory.PathState
				(false, false);

			public static readonly DomainSocketFactory.PathState ShortCircuitDisabled = new DomainSocketFactory.PathState
				(true, false);

			public static readonly DomainSocketFactory.PathState Valid = new DomainSocketFactory.PathState
				(true, true);

			internal PathState(bool usableForDataTransfer, bool usableForShortCircuit)
			{
				this.usableForDataTransfer = usableForDataTransfer;
				this.usableForShortCircuit = usableForShortCircuit;
			}

			public bool GetUsableForDataTransfer()
			{
				return DomainSocketFactory.PathState.usableForDataTransfer;
			}

			public bool GetUsableForShortCircuit()
			{
				return DomainSocketFactory.PathState.usableForShortCircuit;
			}

			private readonly bool usableForDataTransfer;

			private readonly bool usableForShortCircuit;
		}

		public class PathInfo
		{
			private static readonly DomainSocketFactory.PathInfo NotConfigured = new DomainSocketFactory.PathInfo
				(string.Empty, DomainSocketFactory.PathState.Unusable);

			private readonly string path;

			private readonly DomainSocketFactory.PathState state;

			internal PathInfo(string path, DomainSocketFactory.PathState state)
			{
				this.path = path;
				this.state = state;
			}

			public virtual string GetPath()
			{
				return path;
			}

			public virtual DomainSocketFactory.PathState GetPathState()
			{
				return state;
			}

			public override string ToString()
			{
				return new StringBuilder().Append("PathInfo{path=").Append(path).Append(", state="
					).Append(state).Append("}").ToString();
			}
		}

		/// <summary>Information about domain socket paths.</summary>
		internal readonly Com.Google.Common.Cache.Cache<string, DomainSocketFactory.PathState
			> pathMap = CacheBuilder.NewBuilder().ExpireAfterWrite(10, TimeUnit.Minutes).Build
			();

		public DomainSocketFactory(DFSClient.Conf conf)
		{
			string feature;
			if (conf.IsShortCircuitLocalReads() && (!conf.IsUseLegacyBlockReaderLocal()))
			{
				feature = "The short-circuit local reads feature";
			}
			else
			{
				if (conf.IsDomainSocketDataTraffic())
				{
					feature = "UNIX domain socket data traffic";
				}
				else
				{
					feature = null;
				}
			}
			if (feature == null)
			{
				PerformanceAdvisory.Log.Debug("Both short-circuit local reads and UNIX domain socket are disabled."
					);
			}
			else
			{
				if (conf.GetDomainSocketPath().IsEmpty())
				{
					throw new HadoopIllegalArgumentException(feature + " is enabled but " + DFSConfigKeys
						.DfsDomainSocketPathKey + " is not set.");
				}
				else
				{
					if (DomainSocket.GetLoadingFailureReason() != null)
					{
						Log.Warn(feature + " cannot be used because " + DomainSocket.GetLoadingFailureReason
							());
					}
					else
					{
						Log.Debug(feature + " is enabled.");
					}
				}
			}
		}

		/// <summary>Get information about a domain socket path.</summary>
		/// <param name="addr">The inet address to use.</param>
		/// <param name="conf">The client configuration.</param>
		/// <returns>Information about the socket path.</returns>
		public virtual DomainSocketFactory.PathInfo GetPathInfo(IPEndPoint addr, DFSClient.Conf
			 conf)
		{
			// If there is no domain socket path configured, we can't use domain
			// sockets.
			if (conf.GetDomainSocketPath().IsEmpty())
			{
				return DomainSocketFactory.PathInfo.NotConfigured;
			}
			// If we can't do anything with the domain socket, don't create it.
			if (!conf.IsDomainSocketDataTraffic() && (!conf.IsShortCircuitLocalReads() || conf
				.IsUseLegacyBlockReaderLocal()))
			{
				return DomainSocketFactory.PathInfo.NotConfigured;
			}
			// If the DomainSocket code is not loaded, we can't create
			// DomainSocket objects.
			if (DomainSocket.GetLoadingFailureReason() != null)
			{
				return DomainSocketFactory.PathInfo.NotConfigured;
			}
			// UNIX domain sockets can only be used to talk to local peers
			if (!DFSClient.IsLocalAddress(addr))
			{
				return DomainSocketFactory.PathInfo.NotConfigured;
			}
			string escapedPath = DomainSocket.GetEffectivePath(conf.GetDomainSocketPath(), addr
				.Port);
			DomainSocketFactory.PathState status = pathMap.GetIfPresent(escapedPath);
			if (status == null)
			{
				return new DomainSocketFactory.PathInfo(escapedPath, DomainSocketFactory.PathState
					.Valid);
			}
			else
			{
				return new DomainSocketFactory.PathInfo(escapedPath, status);
			}
		}

		public virtual DomainSocket CreateSocket(DomainSocketFactory.PathInfo info, int socketTimeout
			)
		{
			Preconditions.CheckArgument(info.GetPathState() != DomainSocketFactory.PathState.
				Unusable);
			bool success = false;
			DomainSocket sock = null;
			try
			{
				sock = DomainSocket.Connect(info.GetPath());
				sock.SetAttribute(DomainSocket.ReceiveTimeout, socketTimeout);
				success = true;
			}
			catch (IOException e)
			{
				Log.Warn("error creating DomainSocket", e);
			}
			finally
			{
				// fall through
				if (!success)
				{
					if (sock != null)
					{
						IOUtils.CloseQuietly(sock);
					}
					pathMap.Put(info.GetPath(), DomainSocketFactory.PathState.Unusable);
					sock = null;
				}
			}
			return sock;
		}

		public virtual void DisableShortCircuitForPath(string path)
		{
			pathMap.Put(path, DomainSocketFactory.PathState.ShortCircuitDisabled);
		}

		public virtual void DisableDomainSocketPath(string path)
		{
			pathMap.Put(path, DomainSocketFactory.PathState.Unusable);
		}

		[VisibleForTesting]
		public virtual void ClearPathMap()
		{
			pathMap.InvalidateAll();
		}
	}
}
