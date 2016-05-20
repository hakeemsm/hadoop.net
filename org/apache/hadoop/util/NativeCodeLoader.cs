using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>A helper to load the native hadoop code i.e.</summary>
	/// <remarks>
	/// A helper to load the native hadoop code i.e. libhadoop.so.
	/// This handles the fallback to either the bundled libhadoop-Linux-i386-32.so
	/// or the default java implementations where appropriate.
	/// </remarks>
	public class NativeCodeLoader
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.NativeCodeLoader
			)));

		private static bool nativeCodeLoaded = false;

		static NativeCodeLoader()
		{
			// Try to load native hadoop library and set fallback flag appropriately
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Trying to load the custom-built native-hadoop library...");
			}
			try
			{
				Sharpen.Runtime.loadLibrary("hadoop");
				LOG.debug("Loaded the native-hadoop library");
				nativeCodeLoaded = true;
			}
			catch (System.Exception t)
			{
				// Ignore failure to load
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Failed to load native-hadoop with error: " + t);
					LOG.debug("java.library.path=" + Sharpen.Runtime.getProperty("java.library.path")
						);
				}
			}
			if (!nativeCodeLoaded)
			{
				LOG.warn("Unable to load native-hadoop library for your platform... " + "using builtin-java classes where applicable"
					);
			}
		}

		/// <summary>Check if native-hadoop code is loaded for this platform.</summary>
		/// <returns>
		/// <code>true</code> if native-hadoop is loaded,
		/// else <code>false</code>
		/// </returns>
		public static bool isNativeCodeLoaded()
		{
			return nativeCodeLoaded;
		}

		/// <summary>Returns true only if this build was compiled with support for snappy.</summary>
		public static bool buildSupportsSnappy()
		{
		}

		/// <summary>Returns true only if this build was compiled with support for openssl.</summary>
		public static bool buildSupportsOpenssl()
		{
		}

		public static string getLibraryName()
		{
		}

		/// <summary>Return if native hadoop libraries, if present, can be used for this job.
		/// 	</summary>
		/// <param name="conf">configuration</param>
		/// <returns>
		/// <code>true</code> if native hadoop libraries, if present, can be
		/// used for this job; <code>false</code> otherwise.
		/// </returns>
		public virtual bool getLoadNativeLibraries(org.apache.hadoop.conf.Configuration conf
			)
		{
			return conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_DEFAULT);
		}

		/// <summary>Set if native hadoop libraries, if present, can be used for this job.</summary>
		/// <param name="conf">configuration</param>
		/// <param name="loadNativeLibraries">can native hadoop libraries be loaded</param>
		public virtual void setLoadNativeLibraries(org.apache.hadoop.conf.Configuration conf
			, bool loadNativeLibraries)
		{
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, loadNativeLibraries);
		}
	}
}
