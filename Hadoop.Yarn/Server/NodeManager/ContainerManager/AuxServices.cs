using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class AuxServices : AbstractService, ServiceStateChangeListener, EventHandler
		<AuxServicesEvent>
	{
		internal const string StateStoreRootName = "nm-aux-services";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.AuxServices
			));

		protected internal readonly IDictionary<string, AuxiliaryService> serviceMap;

		protected internal readonly IDictionary<string, ByteBuffer> serviceMetaData;

		private readonly Sharpen.Pattern p = Sharpen.Pattern.Compile("^[A-Za-z_]+[A-Za-z0-9_]*$"
			);

		public AuxServices()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.AuxServices
				).FullName)
		{
			serviceMap = Sharpen.Collections.SynchronizedMap(new Dictionary<string, AuxiliaryService
				>());
			serviceMetaData = Sharpen.Collections.SynchronizedMap(new Dictionary<string, ByteBuffer
				>());
		}

		// Obtain services from configuration in init()
		protected internal void AddService(string name, AuxiliaryService service)
		{
			lock (this)
			{
				Log.Info("Adding auxiliary service " + service.GetName() + ", \"" + name + "\"");
				serviceMap[name] = service;
			}
		}

		internal virtual ICollection<AuxiliaryService> GetServices()
		{
			return Sharpen.Collections.UnmodifiableCollection(serviceMap.Values);
		}

		/// <returns>
		/// the meta data for all registered services, that have been started.
		/// If a service has not been started no metadata will be available. The key
		/// is the name of the service as defined in the configuration.
		/// </returns>
		public virtual IDictionary<string, ByteBuffer> GetMetaData()
		{
			IDictionary<string, ByteBuffer> metaClone = new Dictionary<string, ByteBuffer>(serviceMetaData
				.Count);
			lock (serviceMetaData)
			{
				foreach (KeyValuePair<string, ByteBuffer> entry in serviceMetaData)
				{
					metaClone[entry.Key] = entry.Value.Duplicate();
				}
			}
			return metaClone;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			FsPermission storeDirPerms = new FsPermission((short)0x1c0);
			Path stateStoreRoot = null;
			FileSystem stateStoreFs = null;
			bool recoveryEnabled = conf.GetBoolean(YarnConfiguration.NmRecoveryEnabled, YarnConfiguration
				.DefaultNmRecoveryEnabled);
			if (recoveryEnabled)
			{
				stateStoreRoot = new Path(conf.Get(YarnConfiguration.NmRecoveryDir), StateStoreRootName
					);
				stateStoreFs = FileSystem.GetLocal(conf);
			}
			ICollection<string> auxNames = conf.GetStringCollection(YarnConfiguration.NmAuxServices
				);
			foreach (string sName in auxNames)
			{
				try
				{
					Preconditions.CheckArgument(ValidateAuxServiceName(sName), "The ServiceName: " + 
						sName + " set in " + YarnConfiguration.NmAuxServices + " is invalid." + "The valid service name should only contain a-zA-Z0-9_ "
						 + "and can not start with numbers");
					Type sClass = conf.GetClass<AuxiliaryService>(string.Format(YarnConfiguration.NmAuxServiceFmt
						, sName), null);
					if (null == sClass)
					{
						throw new RuntimeException("No class defined for " + sName);
					}
					AuxiliaryService s = ReflectionUtils.NewInstance(sClass, conf);
					// TODO better use s.getName()?
					if (!sName.Equals(s.GetName()))
					{
						Log.Warn("The Auxilurary Service named '" + sName + "' in the " + "configuration is for "
							 + sClass + " which has " + "a name of '" + s.GetName() + "'. Because these are "
							 + "not the same tools trying to send ServiceData and read " + "Service Meta Data may have issues unless the refer to "
							 + "the name in the config.");
					}
					AddService(sName, s);
					if (recoveryEnabled)
					{
						Path storePath = new Path(stateStoreRoot, sName);
						stateStoreFs.Mkdirs(storePath, storeDirPerms);
						s.SetRecoveryPath(storePath);
					}
					s.Init(conf);
				}
				catch (RuntimeException e)
				{
					Log.Fatal("Failed to initialize " + sName, e);
					throw;
				}
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// TODO fork(?) services running as configured user
			//      monitor for health, shutdown/restart(?) if any should die
			foreach (KeyValuePair<string, AuxiliaryService> entry in serviceMap)
			{
				AuxiliaryService service = entry.Value;
				string name = entry.Key;
				service.Start();
				service.RegisterServiceListener(this);
				ByteBuffer meta = service.GetMetaData();
				if (meta != null)
				{
					serviceMetaData[name] = meta;
				}
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			try
			{
				lock (serviceMap)
				{
					foreach (Org.Apache.Hadoop.Service.Service service in serviceMap.Values)
					{
						if (service.GetServiceState() == Service.STATE.Started)
						{
							service.UnregisterServiceListener(this);
							service.Stop();
						}
					}
					serviceMap.Clear();
					serviceMetaData.Clear();
				}
			}
			finally
			{
				base.ServiceStop();
			}
		}

		public virtual void StateChanged(Org.Apache.Hadoop.Service.Service service)
		{
			Log.Fatal("Service " + service.GetName() + " changed state: " + service.GetServiceState
				());
			Stop();
		}

		public virtual void Handle(AuxServicesEvent @event)
		{
			Log.Info("Got event " + @event.GetType() + " for appId " + @event.GetApplicationID
				());
			switch (@event.GetType())
			{
				case AuxServicesEventType.ApplicationInit:
				{
					Log.Info("Got APPLICATION_INIT for service " + @event.GetServiceID());
					AuxiliaryService service = null;
					try
					{
						service = serviceMap[@event.GetServiceID()];
						service.InitializeApplication(new ApplicationInitializationContext(@event.GetUser
							(), @event.GetApplicationID(), @event.GetServiceData()));
					}
					catch (Exception th)
					{
						LogWarningWhenAuxServiceThrowExceptions(service, AuxServicesEventType.ApplicationInit
							, th);
					}
					break;
				}

				case AuxServicesEventType.ApplicationStop:
				{
					foreach (AuxiliaryService serv in serviceMap.Values)
					{
						try
						{
							serv.StopApplication(new ApplicationTerminationContext(@event.GetApplicationID())
								);
						}
						catch (Exception th)
						{
							LogWarningWhenAuxServiceThrowExceptions(serv, AuxServicesEventType.ApplicationStop
								, th);
						}
					}
					break;
				}

				case AuxServicesEventType.ContainerInit:
				{
					foreach (AuxiliaryService serv_1 in serviceMap.Values)
					{
						try
						{
							serv_1.InitializeContainer(new ContainerInitializationContext(@event.GetUser(), @event
								.GetContainer().GetContainerId(), @event.GetContainer().GetResource()));
						}
						catch (Exception th)
						{
							LogWarningWhenAuxServiceThrowExceptions(serv_1, AuxServicesEventType.ContainerInit
								, th);
						}
					}
					break;
				}

				case AuxServicesEventType.ContainerStop:
				{
					foreach (AuxiliaryService serv_2 in serviceMap.Values)
					{
						try
						{
							serv_2.StopContainer(new ContainerTerminationContext(@event.GetUser(), @event.GetContainer
								().GetContainerId(), @event.GetContainer().GetResource()));
						}
						catch (Exception th)
						{
							LogWarningWhenAuxServiceThrowExceptions(serv_2, AuxServicesEventType.ContainerStop
								, th);
						}
					}
					break;
				}

				default:
				{
					throw new RuntimeException("Unknown type: " + @event.GetType());
				}
			}
		}

		private bool ValidateAuxServiceName(string name)
		{
			if (name == null || name.Trim().IsEmpty())
			{
				return false;
			}
			return p.Matcher(name).Matches();
		}

		private void LogWarningWhenAuxServiceThrowExceptions(AuxiliaryService service, AuxServicesEventType
			 eventType, Exception th)
		{
			Log.Warn((null == service ? "The auxService is null" : "The auxService name is " 
				+ service.GetName()) + " and it got an error at event: " + eventType, th);
		}
	}
}
