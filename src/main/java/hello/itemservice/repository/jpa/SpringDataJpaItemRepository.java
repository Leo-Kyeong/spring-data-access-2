package hello.itemservice.repository.jpa;

import hello.itemservice.domain.Item;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface SpringDataJpaItemRepository extends JpaRepository<Item, Long> {

    // 이름 조회
    List<Item> findByItemNameLike(String itemName);

    // 가격 조회
    List<Item> findByPriceLessThanEqual(Integer price);

    // 쿼리 메서드 (이름 + 가격 조회)
    List<Item> findByItemNameLikeAndPriceLessThanEqual(String itemName, Integer price);

    // 쿼리 직접 실행 (이름 + 가격 조회)
    @Query("select i from Item i where i.itemName like :itemName and i.price <= :price")
    List<Item> findItems(@Param("itemName") String itemName, @Param("price") Integer price);
}
