using Sharpen;

namespace org.apache.hadoop.util
{
	public class NativeLibraryChecker
	{
		/// <summary>A tool to test native library availability,</summary>
		public static void Main(string[] args)
		{
			string usage = "NativeLibraryChecker [-a|-h]\n" + "  -a  use -a to check all libraries are available\n"
				 + "      by default just check hadoop library (and\n" + "      winutils.exe on Windows OS) is available\n"
				 + "      exit with error code 1 if check failed\n" + "  -h  print this message\n";
			if (args.Length > 1 || (args.Length == 1 && !(args[0].Equals("-a") || args[0].Equals
				("-h"))))
			{
				System.Console.Error.WriteLine(usage);
				org.apache.hadoop.util.ExitUtil.terminate(1);
			}
			bool checkAll = false;
			if (args.Length == 1)
			{
				if (args[0].Equals("-h"))
				{
					System.Console.Out.WriteLine(usage);
					return;
				}
				checkAll = true;
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			bool nativeHadoopLoaded = org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded
				();
			bool zlibLoaded = false;
			bool snappyLoaded = false;
			// lz4 is linked within libhadoop
			bool lz4Loaded = nativeHadoopLoaded;
			bool bzip2Loaded = org.apache.hadoop.io.compress.bzip2.Bzip2Factory.isNativeBzip2Loaded
				(conf);
			bool openSslLoaded = false;
			bool winutilsExists = false;
			string openSslDetail = string.Empty;
			string hadoopLibraryName = string.Empty;
			string zlibLibraryName = string.Empty;
			string snappyLibraryName = string.Empty;
			string lz4LibraryName = string.Empty;
			string bzip2LibraryName = string.Empty;
			string winutilsPath = null;
			if (nativeHadoopLoaded)
			{
				hadoopLibraryName = org.apache.hadoop.util.NativeCodeLoader.getLibraryName();
				zlibLoaded = org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf
					);
				if (zlibLoaded)
				{
					zlibLibraryName = org.apache.hadoop.io.compress.zlib.ZlibFactory.getLibraryName();
				}
				snappyLoaded = org.apache.hadoop.util.NativeCodeLoader.buildSupportsSnappy() && org.apache.hadoop.io.compress.SnappyCodec
					.isNativeCodeLoaded();
				if (snappyLoaded && org.apache.hadoop.util.NativeCodeLoader.buildSupportsSnappy())
				{
					snappyLibraryName = org.apache.hadoop.io.compress.SnappyCodec.getLibraryName();
				}
				if (org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason() != null)
				{
					openSslDetail = org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason();
					openSslLoaded = false;
				}
				else
				{
					openSslDetail = org.apache.hadoop.crypto.OpensslCipher.getLibraryName();
					openSslLoaded = true;
				}
				if (lz4Loaded)
				{
					lz4LibraryName = org.apache.hadoop.io.compress.Lz4Codec.getLibraryName();
				}
				if (bzip2Loaded)
				{
					bzip2LibraryName = org.apache.hadoop.io.compress.bzip2.Bzip2Factory.getLibraryName
						(conf);
				}
			}
			// winutils.exe is required on Windows
			winutilsPath = org.apache.hadoop.util.Shell.getWinUtilsPath();
			if (winutilsPath != null)
			{
				winutilsExists = true;
			}
			else
			{
				winutilsPath = string.Empty;
			}
			System.Console.Out.WriteLine("Native library checking:");
			System.Console.Out.printf("hadoop:  %b %s%n", nativeHadoopLoaded, hadoopLibraryName
				);
			System.Console.Out.printf("zlib:    %b %s%n", zlibLoaded, zlibLibraryName);
			System.Console.Out.printf("snappy:  %b %s%n", snappyLoaded, snappyLibraryName);
			System.Console.Out.printf("lz4:     %b %s%n", lz4Loaded, lz4LibraryName);
			System.Console.Out.printf("bzip2:   %b %s%n", bzip2Loaded, bzip2LibraryName);
			System.Console.Out.printf("openssl: %b %s%n", openSslLoaded, openSslDetail);
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				System.Console.Out.printf("winutils: %b %s%n", winutilsExists, winutilsPath);
			}
			if ((!nativeHadoopLoaded) || (org.apache.hadoop.util.Shell.WINDOWS && (!winutilsExists
				)) || (checkAll && !(zlibLoaded && snappyLoaded && lz4Loaded && bzip2Loaded)))
			{
				// return 1 to indicated check failed
				org.apache.hadoop.util.ExitUtil.terminate(1);
			}
		}
	}
}
