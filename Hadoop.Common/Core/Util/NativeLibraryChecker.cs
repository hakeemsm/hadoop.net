using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Compress.Bzip2;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Sharpen;

namespace Org.Apache.Hadoop.Util
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
				ExitUtil.Terminate(1);
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
			Configuration conf = new Configuration();
			bool nativeHadoopLoaded = NativeCodeLoader.IsNativeCodeLoaded();
			bool zlibLoaded = false;
			bool snappyLoaded = false;
			// lz4 is linked within libhadoop
			bool lz4Loaded = nativeHadoopLoaded;
			bool bzip2Loaded = Bzip2Factory.IsNativeBzip2Loaded(conf);
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
				hadoopLibraryName = NativeCodeLoader.GetLibraryName();
				zlibLoaded = ZlibFactory.IsNativeZlibLoaded(conf);
				if (zlibLoaded)
				{
					zlibLibraryName = ZlibFactory.GetLibraryName();
				}
				snappyLoaded = NativeCodeLoader.BuildSupportsSnappy() && SnappyCodec.IsNativeCodeLoaded
					();
				if (snappyLoaded && NativeCodeLoader.BuildSupportsSnappy())
				{
					snappyLibraryName = SnappyCodec.GetLibraryName();
				}
				if (OpensslCipher.GetLoadingFailureReason() != null)
				{
					openSslDetail = OpensslCipher.GetLoadingFailureReason();
					openSslLoaded = false;
				}
				else
				{
					openSslDetail = OpensslCipher.GetLibraryName();
					openSslLoaded = true;
				}
				if (lz4Loaded)
				{
					lz4LibraryName = Lz4Codec.GetLibraryName();
				}
				if (bzip2Loaded)
				{
					bzip2LibraryName = Bzip2Factory.GetLibraryName(conf);
				}
			}
			// winutils.exe is required on Windows
			winutilsPath = Shell.GetWinUtilsPath();
			if (winutilsPath != null)
			{
				winutilsExists = true;
			}
			else
			{
				winutilsPath = string.Empty;
			}
			System.Console.Out.WriteLine("Native library checking:");
			System.Console.Out.Printf("hadoop:  %b %s%n", nativeHadoopLoaded, hadoopLibraryName
				);
			System.Console.Out.Printf("zlib:    %b %s%n", zlibLoaded, zlibLibraryName);
			System.Console.Out.Printf("snappy:  %b %s%n", snappyLoaded, snappyLibraryName);
			System.Console.Out.Printf("lz4:     %b %s%n", lz4Loaded, lz4LibraryName);
			System.Console.Out.Printf("bzip2:   %b %s%n", bzip2Loaded, bzip2LibraryName);
			System.Console.Out.Printf("openssl: %b %s%n", openSslLoaded, openSslDetail);
			if (Shell.Windows)
			{
				System.Console.Out.Printf("winutils: %b %s%n", winutilsExists, winutilsPath);
			}
			if ((!nativeHadoopLoaded) || (Shell.Windows && (!winutilsExists)) || (checkAll &&
				 !(zlibLoaded && snappyLoaded && lz4Loaded && bzip2Loaded)))
			{
				// return 1 to indicated check failed
				ExitUtil.Terminate(1);
			}
		}
	}
}
