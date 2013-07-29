package com.esri;

/**
 */
public final class GeoHash
{
    private GeoHash()
    {
    }

    public static long geohash(
            final double x,
            final double y,
            final double xmin,
            final double ymin,
            final double nume)
    {
        final long gx = (long) Math.floor((x - xmin) * nume);
        final long gy = (long) Math.floor((y - ymin) * nume);
        return (gx << 32) | gy;
    }

    public static double toX(
            final long g,
            final double xmin,
            final double cell
    )
    {
        return (g >> 32) * cell + xmin;
    }

    public static double toY(
            final long g,
            final double ymin,
            final double cell
    )
    {
        return (g & 0xFFFF) * cell + ymin;
    }
}
