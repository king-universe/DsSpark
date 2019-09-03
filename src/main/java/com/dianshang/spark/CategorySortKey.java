package com.dianshang.spark;

import scala.math.Ordered;

import java.io.Serializable;

public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private long clickCount;//点击次数
    private long orderCount;//下单次数
    private long payCount;//支付次数

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if (clickCount < that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount < that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount < that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if (clickCount > that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount > that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount > that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if ($less(that)) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if ($greater(that)) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
