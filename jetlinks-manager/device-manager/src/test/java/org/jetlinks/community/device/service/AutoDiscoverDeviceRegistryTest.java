package org.jetlinks.community.device.service;

import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.device.entity.DeviceProductEntity;
import org.jetlinks.community.device.enums.DeviceState;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceProductOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.ProductInfo;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AutoDiscoverDeviceRegistryTest {

    @Test
    void shouldRestoreActiveProductFromRepository() {
        DeviceRegistry parent = mock(DeviceRegistry.class);
        ReactiveRepository<DeviceInstanceEntity, String> deviceRepository = mock(ReactiveRepository.class);
        ReactiveRepository<DeviceProductEntity, String> productRepository = mock(ReactiveRepository.class);
        DeviceProductOperator productOperator = mock(DeviceProductOperator.class);

        DeviceProductEntity product = new DeviceProductEntity();
        product.setId("hc_fdc");
        product.setState((byte) 1);
        product.setMessageProtocol("hc_fdc");

        when(parent.getProduct("hc_fdc")).thenReturn(Mono.empty());
        when(productRepository.findById("hc_fdc")).thenReturn(Mono.just(product));
        when(parent.register(any(ProductInfo.class))).thenReturn(Mono.just(productOperator));

        AutoDiscoverDeviceRegistry registry =
            new AutoDiscoverDeviceRegistry(parent, deviceRepository, productRepository);

        StepVerifier.create(registry.getProduct("hc_fdc"))
            .expectNext(productOperator)
            .verifyComplete();
    }

    @Test
    void shouldNotRestoreInactiveProduct() {
        DeviceRegistry parent = mock(DeviceRegistry.class);
        ReactiveRepository<DeviceInstanceEntity, String> deviceRepository = mock(ReactiveRepository.class);
        ReactiveRepository<DeviceProductEntity, String> productRepository = mock(ReactiveRepository.class);

        DeviceProductEntity product = new DeviceProductEntity();
        product.setId("hc_fdc");
        product.setState((byte) 0);

        when(parent.getProduct("hc_fdc")).thenReturn(Mono.empty());
        when(productRepository.findById("hc_fdc")).thenReturn(Mono.just(product));

        AutoDiscoverDeviceRegistry registry =
            new AutoDiscoverDeviceRegistry(parent, deviceRepository, productRepository);

        StepVerifier.create(registry.getProduct("hc_fdc"))
            .verifyComplete();

        verify(parent, never()).register(any(ProductInfo.class));
    }

    @Test
    void shouldRestoreProductBeforeDevice() {
        DeviceRegistry parent = mock(DeviceRegistry.class);
        ReactiveRepository<DeviceInstanceEntity, String> deviceRepository = mock(ReactiveRepository.class);
        ReactiveRepository<DeviceProductEntity, String> productRepository = mock(ReactiveRepository.class);
        DeviceProductOperator productOperator = mock(DeviceProductOperator.class);
        DeviceOperator deviceOperator = mock(DeviceOperator.class);

        DeviceProductEntity product = new DeviceProductEntity();
        product.setId("hc_fdc");
        product.setState((byte) 1);
        product.setMessageProtocol("hc_fdc");

        DeviceInstanceEntity device = new DeviceInstanceEntity();
        device.setId("FDC001");
        device.setProductId("hc_fdc");
        device.setState(DeviceState.online);

        when(parent.getDevice("FDC001")).thenReturn(Mono.empty());
        when(deviceRepository.findById("FDC001")).thenReturn(Mono.just(device));
        when(parent.getProduct("hc_fdc")).thenReturn(Mono.empty());
        when(productRepository.findById("hc_fdc")).thenReturn(Mono.just(product));
        when(parent.register(any(ProductInfo.class))).thenReturn(Mono.just(productOperator));
        when(parent.register(any(DeviceInfo.class))).thenReturn(Mono.just(deviceOperator));

        AutoDiscoverDeviceRegistry registry =
            new AutoDiscoverDeviceRegistry(parent, deviceRepository, productRepository);

        StepVerifier.create(registry.getDevice("FDC001"))
            .expectNext(deviceOperator)
            .verifyComplete();

        InOrder order = inOrder(parent);
        order.verify(parent).register(any(ProductInfo.class));
        order.verify(parent).register(any(DeviceInfo.class));
    }

    @Test
    void shouldNotRestoreDeviceWhenProductIsInactive() {
        DeviceRegistry parent = mock(DeviceRegistry.class);
        ReactiveRepository<DeviceInstanceEntity, String> deviceRepository = mock(ReactiveRepository.class);
        ReactiveRepository<DeviceProductEntity, String> productRepository = mock(ReactiveRepository.class);

        DeviceProductEntity product = new DeviceProductEntity();
        product.setId("hc_fdc");
        product.setState((byte) 0);

        DeviceInstanceEntity device = new DeviceInstanceEntity();
        device.setId("FDC001");
        device.setProductId("hc_fdc");
        device.setState(DeviceState.online);

        when(parent.getDevice("FDC001")).thenReturn(Mono.empty());
        when(deviceRepository.findById("FDC001")).thenReturn(Mono.just(device));
        when(parent.getProduct("hc_fdc")).thenReturn(Mono.empty());
        when(productRepository.findById("hc_fdc")).thenReturn(Mono.just(product));

        AutoDiscoverDeviceRegistry registry =
            new AutoDiscoverDeviceRegistry(parent, deviceRepository, productRepository);

        StepVerifier.create(registry.getDevice("FDC001"))
            .verifyComplete();

        verify(parent, never()).register(any(DeviceInfo.class));
    }
}
