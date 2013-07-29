package com.esri;

import java.io.Serializable;

/**
 */
public final class KDItem implements Serializable
{
    private static final long serialVersionUID = 6805260676602620224L;

    public long geohash;
    public int value;

    public KDItem()
    {
    }

    public KDItem(
            final long geohash,
            final int value)
    {
        this.geohash = geohash;
        this.value = value;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof KDItem))
        {
            return false;
        }

        final KDItem kdItem = (KDItem) o;

        if (geohash != kdItem.geohash)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (geohash ^ (geohash >>> 32));
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("KDItem{");
        sb.append("geohash=").append(geohash);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
