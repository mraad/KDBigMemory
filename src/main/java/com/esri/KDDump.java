package com.esri;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.terracotta.toolkit.Toolkit;
import org.terracotta.toolkit.ToolkitFactory;
import org.terracotta.toolkit.ToolkitInstantiationException;

import java.io.IOException;
import java.util.Map;

/**
 * mvn -q exec:java -Dexec.mainClass=com.esri.KDDump -Dexec.args="infousa"
 */
public final class KDDump
{
    private final static Log m_log = LogFactory.getLog(KDDump.class);

    public final static void main(String[] args) throws ToolkitInstantiationException, IOException
    {
        final Toolkit toolkit = ToolkitFactory.createToolkit(KDConst.TOOLKIT_URI_VAL);
        try
        {
            final String worldName = args.length == 0 ? "infousa" : args[0];
            // Get the job arguments.
            final Map<String, Double> map = toolkit.getMap(worldName + "Map", String.class, Double.class);
            if (map != null && map.size() > 0)
            {
                final double xmin = map.get(KDConst.XMIN_KEY);
                final double ymin = map.get(KDConst.YMIN_KEY);
                final double cell = map.get(KDConst.CELL_KEY);
                // Iterate over the job output result.
                for (final KDItem item : toolkit.getSet(worldName + "Set", KDItem.class))
                {
                    System.out.format("%.1f\t%.1f\t%d\n",
                            GeoHash.toX(item.geohash, xmin, cell),
                            GeoHash.toY(item.geohash, ymin, cell),
                            item.value);
                }
            }
        }
        finally
        {
            toolkit.shutdown();
        }
    }
}