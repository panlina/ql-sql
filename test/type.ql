store {
	store_id: number;
	address_id: number;
	address = address#(this store.address_id);
	id store_id;
}
address {
	address_id: number;
	city_id: number;
	city = city#(this address.city_id);
	id address_id;
}
city {
	city_id: number;
	country_id: number;
	country = country#(this city.country_id);
	id city_id;
}
country {
	country_id: number;
	country: string;
	id country_id;
}
actor {
	actor_id: number;
	first_name: string;
	last_name: string;
	films = ::film where (::film_actor where film_id=this film.film_id & actor_id=this actor.actor_id);
	id actor_id;
}
category {
	category_id: number;
	name: string;
	films = ::film where (::film_category where film_id=this film.film_id & category_id=this category.category_id);
	id category_id;
}
film {
	film_id: number;
	title: string;
	length: number;
	actors = ::actor where (::film_actor where actor_id=this actor.actor_id & film_id=this film.film_id);
	categories = ::category where (::film_category where category_id=this category.category_id & film_id=this film.film_id);
	id film_id;
}
film_actor {
	actor_id: number;
	film_id: number;
}
film_category {
	category_id: number;
	film_id: number;
}
inventory {
	inventory_id: number;
	film_id: number;
	store_id: number;
	film = film#(this inventory.film_id);
	store = store#(this inventory.store_id);
	id inventory_id;
}
customer {
	customer_id: number;
	store_id: number;
	first_name: string;
	last_name: string;
	address_id: number;
	active: boolean;
	store = store#(this customer.store_id);
	address = address#(this customer.address_id);
	id customer_id;
}
