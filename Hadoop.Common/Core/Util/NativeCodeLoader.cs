using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>A helper to load the native hadoop code i.e.</summary>
	/// <remarks>
	/// A helper to load the native hadoop code i.e. libhadoop.so.
	/// This handles the fallback to either the bundled libhadoop-Linux-i386-32.so
	/// or the default java implementations where appropriate.
	/// </remarks>
	public class NativeCodeLoader
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(NativeCodeLoader));

		private static bool nativeCodeLoaded = false;

		static NativeCodeLoader()
		{
			// Try to load native hadoop library and set fallback flag appropriately
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Trying to load the custom-built native-hadoop library...");
			}
			try
			{
				Runtime.LoadLibrary("hadoop");
				Log.Debug("Loaded the native-hadoop library");
				nativeCodeLoaded = true;
			}
			catch (Exception t)
			{
				// Ignore failure to load
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Failed to load native-hadoop with error: " + t);
					Log.Debug("java.library.path=" + Runtime.GetProperty("java.library.path"));
				}
			}
			if (!nativeCodeLoaded)
			{
				Log.Warn("Unable to load native-hadoop library for your platform... " + "using builtin-java classes where applicable"
					);
			}
		}

		/// <summary>Check if native-hadoop code is loaded for this platform.</summary>
		/// <returns>
		/// <code>true</code> if native-hadoop is loaded,
		/// else <code>false</code>
		/// </returns>
		public static bool IsNativeCodeLoaded()
		{
			return nativeCodeLoaded;
		}

		/// <summary>Returns true only if this build was compiled with support for snappy.</summary>
		public static bool BuildSupportsSnappy()
		{
		}

		/// <summary>Returns true only if this build was compiled with support for openssl.</summary>
		public static bool BuildSupportsOpenssl()
		{
		}

		public static string GetLibraryName()
		{
		}

		/// <summary>Return if native hadoop libraries, if present, can be used for this job.
		/// 	</summary>
		/// <param name="conf">configuration</param>
		/// <returns>
		/// <code>true</code> if native hadoop libraries, if present, can be
		/// used for this job; <code>false</code> otherwise.
		/// </returns>
		public virtual bool GetLoadNativeLibraries(Configuration conf)
		{
			return conf.GetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, CommonConfigurationKeys
				.IoNativeLibAvailableDefault);
		}

		/// <summary>Set if native hadoop libraries, if present, can be used for this job.</summary>
		/// <param name="conf">configuration</param>
		/// <param name="loadNativeLibraries">can native hadoop libraries be loaded</param>
		public virtual void SetLoadNativeLibraries(Configuration conf, bool loadNativeLibraries
			)
		{
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, loadNativeLibraries
				);
		}
	}
}
