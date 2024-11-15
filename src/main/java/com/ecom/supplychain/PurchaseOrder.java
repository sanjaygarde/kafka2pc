package com.ecom.supplychain;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Date;

public class PurchaseOrder {
	@JsonProperty
	private String orderId;

	@JsonProperty
	private String purchaserId;
	@JsonProperty
	private String itemId;

	@JsonProperty
	private double price;

	@JsonProperty
	private int quantity;

	@JsonProperty
	//@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm a z")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	private Date orderDate;

	public PurchaseOrder(){}

	public PurchaseOrder(String orderId, String clientId, String itemId, double price, int quantity, Date orderDate) {
		this.orderId = orderId;
		this.purchaserId = clientId;
		this.itemId = itemId;
		this.setPrice(price);
		this.quantity = quantity;
		this.setOrderDate(orderDate);
	}

	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getItemId() {
		return itemId;
	}
	public void setItemId(String itemId) {
		this.itemId = itemId;
	}
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	public String getPurchaserId() {
		return purchaserId;
	}
	public void setPurchaserId(String purchaserId) {
		this.purchaserId = purchaserId;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	public Date getOrderDate() {
		return orderDate;
	}

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	public void setOrderDate(Date orderDate) {
		this.orderDate = orderDate;
	}

}
